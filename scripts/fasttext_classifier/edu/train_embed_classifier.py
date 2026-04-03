"""
Fine-tune Qwen3-Embedding-0.6B as a binary educational classifier.

Binary classification: score >= threshold → edu_high (1), else edu_low (0).
Supports multi-GPU via DDP. Early stopping on validation F1.

Usage:
    # Single GPU
    python train_edu_classifier.py \
        --input workdir/train_balanced_t3_Qwen3-235B.jsonl \
        --eval-file workdir/eval_10M_10.jsonl \
        --output workdir/edu_finetune_t3

    # Multi-GPU (8 GPUs)
    torchrun --nproc_per_node=8 train_edu_classifier.py \
        --input workdir/train_balanced_t3_Qwen3-235B.jsonl \
        --eval-file workdir/eval_10M_10.jsonl \
        --output workdir/edu_finetune_t3
"""

import argparse
import json
import os
import random
from pathlib import Path

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as F
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader, Dataset, DistributedSampler
from tqdm import tqdm
from transformers import AutoModel, AutoTokenizer


# --- Distributed helpers ---

def is_distributed():
    return dist.is_available() and dist.is_initialized()


def get_rank():
    return dist.get_rank() if is_distributed() else 0


def get_world_size():
    return dist.get_world_size() if is_distributed() else 1


def is_main():
    return get_rank() == 0


def log(msg):
    if is_main():
        print(msg, flush=True)


# --- Model ---

class EduClassifier(nn.Module):
    def __init__(self, encoder, hidden_size):
        super().__init__()
        self.encoder = encoder
        self.head = nn.Linear(hidden_size, 1)

    def forward(self, input_ids, attention_mask):
        outputs = self.encoder(input_ids=input_ids, attention_mask=attention_mask)
        # Last token pooling (for decoder-based embedding models with left padding)
        left_padding = attention_mask[:, -1].sum() == attention_mask.shape[0]
        if left_padding:
            embeddings = outputs.last_hidden_state[:, -1]
        else:
            seq_lengths = attention_mask.sum(dim=1) - 1
            batch_size = outputs.last_hidden_state.shape[0]
            embeddings = outputs.last_hidden_state[
                torch.arange(batch_size, device=outputs.last_hidden_state.device), seq_lengths
            ]
        embeddings = F.normalize(embeddings, p=2, dim=1)
        return self.head(embeddings).squeeze(-1)


# --- Dataset ---

class TextDataset(Dataset):
    def __init__(self, texts, labels):
        self.texts = texts
        self.labels = labels

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        return self.texts[idx], self.labels[idx]


def collate_fn(batch, tokenizer, max_length):
    texts, labels = zip(*batch)
    encoded = tokenizer(
        list(texts),
        max_length=max_length,
        truncation=True,
        padding=True,
        return_tensors="pt",
    )
    return encoded["input_ids"], encoded["attention_mask"], torch.tensor(labels, dtype=torch.float32)


# --- Data loading ---

def load_docs(path: str, edu_threshold: int) -> list[dict]:
    """Load labeled JSONL, convert to binary labels."""
    docs = []
    skipped = 0
    score_dist = [0] * 6
    with open(path) as f:
        for line in f:
            doc = json.loads(line)
            if doc.get("label") == "ERROR" or doc.get("score") is None:
                skipped += 1
                continue
            score = int(doc["score"])
            if not 0 <= score <= 5:
                skipped += 1
                continue
            score_dist[score] += 1
            label = 1 if score >= edu_threshold else 0
            docs.append({"text": doc["text"], "label": label, "score": score})

    log(f"Loaded {len(docs):,} documents (skipped {skipped:,} errors)")
    log("Score distribution:")
    for s in range(6):
        pct = score_dist[s] / len(docs) * 100 if docs else 0
        marker = " ← threshold" if s == edu_threshold else ""
        log(f"  {s}: {score_dist[s]:>7,} ({pct:5.1f}%){marker}")
    high = sum(1 for d in docs if d["label"] == 1)
    log(f"  edu_high: {high:,} ({100*high/len(docs):.1f}%), edu_low: {len(docs)-high:,} ({100*(len(docs)-high)/len(docs):.1f}%)")
    return docs


# --- Evaluation ---

@torch.no_grad()
def compute_f1(model, dataloader, device, threshold=0.5):
    """Compute binary F1 for edu_high on a dataloader."""
    model.eval()
    tp = fp = fn = tn = 0
    for input_ids, attention_mask, labels in dataloader:
        input_ids = input_ids.to(device)
        attention_mask = attention_mask.to(device)
        labels = labels.to(device)
        with torch.autocast(device_type="cuda", dtype=torch.float16, enabled=(device.type == torch.device("cuda").type)):
            logits = model(input_ids, attention_mask)
        preds = (torch.sigmoid(logits) >= threshold).long()
        targets = labels.long()
        tp += ((preds == 1) & (targets == 1)).sum().item()
        fp += ((preds == 1) & (targets == 0)).sum().item()
        fn += ((preds == 0) & (targets == 1)).sum().item()
        tn += ((preds == 0) & (targets == 0)).sum().item()

    p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0.0
    acc = (tp + tn) / (tp + fp + fn + tn) if (tp + fp + fn + tn) > 0 else 0.0
    return {"precision": p, "recall": r, "f1": f1, "accuracy": acc, "tp": tp, "fp": fp, "fn": fn, "tn": tn}


@torch.no_grad()
def full_eval(model, dataloader, device):
    """Full evaluation with threshold sweep."""
    model.eval()
    all_probs = []
    all_labels = []
    all_scores = []

    for input_ids, attention_mask, labels in tqdm(dataloader, desc="Evaluating", disable=not is_main()):
        input_ids = input_ids.to(device)
        attention_mask = attention_mask.to(device)
        with torch.autocast(device_type="cuda", dtype=torch.float16, enabled=(device.type == torch.device("cuda").type)):
            logits = model(input_ids, attention_mask)
        probs = torch.sigmoid(logits).cpu()
        all_probs.append(probs)
        all_labels.append(labels)

    all_probs = torch.cat(all_probs)
    all_labels = torch.cat(all_labels)

    log("\nConfidence threshold sweep (edu_high):")
    log(f"  {'thresh':>6s}  {'P':>6s}  {'R':>6s}  {'F1':>6s}  {'Acc':>6s}  {'TP':>8s}  {'FP':>8s}  {'FN':>8s}")
    best_f1 = 0
    best_thresh = 0.5
    for t in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        preds = (all_probs >= t).long()
        targets = all_labels.long()
        tp = ((preds == 1) & (targets == 1)).sum().item()
        fp = ((preds == 1) & (targets == 0)).sum().item()
        fn = ((preds == 0) & (targets == 1)).sum().item()
        tn = ((preds == 0) & (targets == 0)).sum().item()
        p = tp / (tp + fp) if (tp + fp) > 0 else 0
        r = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0
        acc = (tp + tn) / (tp + fp + fn + tn)
        if f1 > best_f1:
            best_f1 = f1
            best_thresh = t
        log(f"  {t:>6.1f}  {p:>6.3f}  {r:>6.3f}  {f1:>6.3f}  {acc:>6.3f}  {tp:>8,}  {fp:>8,}  {fn:>8,}")

    log(f"\n  Best F1: {best_f1:.3f} at threshold {best_thresh}")
    return {"best_f1": best_f1, "best_threshold": best_thresh}


# --- Main ---

def main():
    parser = argparse.ArgumentParser(description="Fine-tune embedding model for binary edu classification")
    parser.add_argument("--input", required=True, help="Training JSONL with 'score' field (0-5)")
    parser.add_argument("--output", required=True, help="Output directory for saved model")
    parser.add_argument("--eval-file", type=str, default=None, help="Eval JSONL file")
    parser.add_argument("--eval-split", type=float, default=0.1, help="Eval split if no --eval-file")
    parser.add_argument("--val-samples", type=int, default=10000, help="Subsample from eval for early stopping")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--batch-size", type=int, default=256, help="Per-GPU batch size")
    parser.add_argument("--grad-accum", type=int, default=1, help="Gradient accumulation steps")
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--lr", type=float, default=2e-5, help="Learning rate for encoder")
    parser.add_argument("--head-lr", type=float, default=1e-3, help="Learning rate for classification head")
    parser.add_argument("--warmup-steps", type=int, default=100)
    parser.add_argument("--encoder", default="Qwen/Qwen3-Embedding-0.6B")
    parser.add_argument("--edu-threshold", type=int, default=3, help="Score >= T → edu_high")
    parser.add_argument("--max-length", type=int, default=512)
    parser.add_argument("--patience", type=int, default=3, help="Early stopping patience (epochs)")
    parser.add_argument("--language", type=str, default=None)
    parser.add_argument("--val-every-n-steps", type=int, default=None, help="Validate every N steps (default: once per epoch)")
    args = parser.parse_args()

    # Init distributed
    if "RANK" in os.environ:
        dist.init_process_group(backend="nccl")
        local_rank = int(os.environ["LOCAL_RANK"])
        torch.cuda.set_device(local_rank)
        device = torch.device(f"cuda:{local_rank}")
    else:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    random.seed(args.seed)
    torch.manual_seed(args.seed)

    # Load data
    docs = load_docs(args.input, args.edu_threshold)
    if args.language:
        before = len(docs)
        docs = [d for d in docs if d.get("language") == args.language]
        log(f"Filtered to language '{args.language}': {before:,} → {len(docs):,}")

    random.shuffle(docs)
    if args.eval_file:
        train_docs = docs
        eval_docs = load_docs(args.eval_file, args.edu_threshold)
    else:
        split_idx = int(len(docs) * (1 - args.eval_split))
        train_docs = docs[:split_idx]
        eval_docs = docs[split_idx:]

    # Subsample eval for validation (early stopping)
    random.shuffle(eval_docs)
    val_size = min(args.val_samples, len(eval_docs))
    val_docs = eval_docs[:val_size]
    log(f"\nTrain: {len(train_docs):,}, Val (early stopping): {val_size:,}, Full eval: {len(eval_docs):,}")

    # Datasets
    train_dataset = TextDataset([d["text"] for d in train_docs], [d["label"] for d in train_docs])
    val_dataset = TextDataset([d["text"] for d in val_docs], [d["label"] for d in val_docs])
    eval_dataset = TextDataset([d["text"] for d in eval_docs], [d["label"] for d in eval_docs])

    # Load model
    log(f"\nLoading encoder: {args.encoder}")
    tokenizer = AutoTokenizer.from_pretrained(args.encoder, padding_side="left")
    encoder = AutoModel.from_pretrained(args.encoder)
    encoder.gradient_checkpointing_enable()
    hidden_size = encoder.config.hidden_size
    model = EduClassifier(encoder, hidden_size).to(device)
    log(f"Hidden size: {hidden_size}, World size: {get_world_size()}")

    # DDP
    if is_distributed():
        model = DDP(model, device_ids=[device.index], find_unused_parameters=False)

    # Dataloaders
    collate = lambda batch: collate_fn(batch, tokenizer, args.max_length)
    train_sampler = DistributedSampler(train_dataset, shuffle=True) if is_distributed() else None
    train_loader = DataLoader(
        train_dataset, batch_size=args.batch_size, sampler=train_sampler,
        shuffle=(train_sampler is None), collate_fn=collate, num_workers=4, pin_memory=True,
    )
    val_loader = DataLoader(val_dataset, batch_size=args.batch_size * 2, shuffle=False, collate_fn=collate, num_workers=4, pin_memory=True)
    eval_loader = DataLoader(eval_dataset, batch_size=args.batch_size * 2, shuffle=False, collate_fn=collate, num_workers=4, pin_memory=True)

    # Optimizer with separate LRs
    raw_model = model.module if is_distributed() else model
    optimizer = torch.optim.AdamW([
        {"params": raw_model.encoder.parameters(), "lr": args.lr},
        {"params": raw_model.head.parameters(), "lr": args.head_lr},
    ], weight_decay=0.01)

    # Linear warmup + cosine decay
    total_steps = len(train_loader) * args.epochs // args.grad_accum
    def lr_lambda(step):
        if step < args.warmup_steps:
            return step / max(args.warmup_steps, 1)
        progress = (step - args.warmup_steps) / max(total_steps - args.warmup_steps, 1)
        return 0.5 * (1 + torch.cos(torch.tensor(progress * 3.14159)).item())

    scheduler = torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)
    scaler = torch.amp.GradScaler(enabled=(device.type == "cuda"))
    loss_fn = nn.BCEWithLogitsLoss()

    log(f"\nTraining for {args.epochs} epochs ({total_steps} optimizer steps)")
    log(f"  Batch size: {args.batch_size} x {get_world_size()} GPUs x {args.grad_accum} accum = {args.batch_size * get_world_size() * args.grad_accum} effective")

    # Training loop with early stopping
    best_f1 = 0.0
    patience_counter = 0
    global_step = 0
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    for epoch in range(1, args.epochs + 1):
        if train_sampler is not None:
            train_sampler.set_epoch(epoch)
        model.train()
        total_loss = 0.0
        n_batches = 0
        optimizer.zero_grad()

        pbar = tqdm(train_loader, desc=f"Epoch {epoch}", disable=not is_main())
        for step, (input_ids, attention_mask, labels) in enumerate(pbar):
            input_ids = input_ids.to(device)
            attention_mask = attention_mask.to(device)
            labels = labels.to(device)

            with torch.autocast(device_type="cuda", dtype=torch.float16, enabled=(device.type == "cuda")):
                logits = model(input_ids, attention_mask)
                loss = loss_fn(logits, labels) / args.grad_accum

            scaler.scale(loss).backward()
            total_loss += loss.item() * args.grad_accum
            n_batches += 1

            if (step + 1) % args.grad_accum == 0:
                scaler.unscale_(optimizer)
                nn.utils.clip_grad_norm_(model.parameters(), 1.0)
                scaler.step(optimizer)
                scaler.update()
                optimizer.zero_grad()
                scheduler.step()
                global_step += 1

                if is_main():
                    pbar.set_postfix(loss=f"{total_loss/n_batches:.4f}", lr=f"{scheduler.get_last_lr()[0]:.2e}")

                # Mid-epoch validation
                if args.val_every_n_steps and global_step % args.val_every_n_steps == 0 and is_main():
                    val_metrics = compute_f1(raw_model, val_loader, device)
                    log(f"  Step {global_step}: val P={val_metrics['precision']:.3f} R={val_metrics['recall']:.3f} F1={val_metrics['f1']:.3f}")
                    model.train()

        avg_loss = total_loss / n_batches

        # End-of-epoch validation (rank 0 only)
        if is_main():
            val_metrics = compute_f1(raw_model, val_loader, device)
            log(f"Epoch {epoch}: loss={avg_loss:.4f} | val P={val_metrics['precision']:.3f} R={val_metrics['recall']:.3f} F1={val_metrics['f1']:.3f}")

            if val_metrics["f1"] > best_f1:
                best_f1 = val_metrics["f1"]
                patience_counter = 0
                # Save best model
                torch.save(raw_model.state_dict(), output_dir / "best_model.pt")
                log(f"  New best F1: {best_f1:.3f} — saved checkpoint")
            else:
                patience_counter += 1
                log(f"  No improvement ({patience_counter}/{args.patience})")

            should_stop = patience_counter >= args.patience
        else:
            should_stop = False

        # Broadcast early stop decision
        if is_distributed():
            stop_tensor = torch.tensor([int(should_stop)], device=device)
            dist.broadcast(stop_tensor, src=0)
            should_stop = stop_tensor.item() == 1

        if should_stop:
            log(f"\nEarly stopping at epoch {epoch} (best val F1: {best_f1:.3f})")
            break

    # Final evaluation on full eval set (rank 0 only)
    if is_main():
        log("\nLoading best checkpoint for final evaluation...")
        raw_model.load_state_dict(torch.load(output_dir / "best_model.pt", weights_only=True))
        raw_model.to(device)

        log(f"\nFull eval ({len(eval_docs):,} docs):")
        val_metrics = compute_f1(raw_model, eval_loader, device)
        log(f"  P={val_metrics['precision']:.3f} R={val_metrics['recall']:.3f} F1={val_metrics['f1']:.3f} Acc={val_metrics['accuracy']:.3f}")

        sweep_results = full_eval(raw_model, eval_loader, device)

        # Save final artifacts
        config = {
            "encoder": args.encoder,
            "hidden_size": hidden_size,
            "edu_threshold": args.edu_threshold,
            "max_length": args.max_length,
            "train_size": len(train_docs),
            "eval_size": len(eval_docs),
            "val_size": val_size,
            "epochs_trained": epoch,
            "best_val_f1": best_f1,
            "lr": args.lr,
            "head_lr": args.head_lr,
            "batch_size": args.batch_size,
            "grad_accum": args.grad_accum,
            "seed": args.seed,
        }
        config.update(sweep_results)
        (output_dir / "config.json").write_text(json.dumps(config, indent=2))
        (output_dir / "eval_results.json").write_text(json.dumps(val_metrics, indent=2))

        # Save encoder + head separately for easy loading
        raw_model.encoder.save_pretrained(output_dir / "encoder")
        tokenizer.save_pretrained(output_dir / "encoder")
        torch.save(raw_model.head.state_dict(), output_dir / "head.pt")

        log(f"\nModel saved to {output_dir}/")
        log(f"  best_model.pt — Full model checkpoint")
        log(f"  encoder/      — Fine-tuned encoder (HF format)")
        log(f"  head.pt       — Classification head weights")
        log(f"  config.json   — Training config + best threshold")

    if is_distributed():
        dist.destroy_process_group()


if __name__ == "__main__":
    main()

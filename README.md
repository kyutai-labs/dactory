# dactory

The data factory!

## Installation

The recommended way to use this package is with [uv](https://docs.astral.sh/uv/). But it's also compatible with pip.

In both cases, you need the Cargo (the Rust compiler) to use this package. Install it with:

```bash
curl https://sh.rustup.rs -sSf | sh
```

### Installation with uv

If you want to install uv, run
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

With uv, you have the choice of just calling the tool with uv, or cloning the repository to edit it.

#### I just want to use the tool
```bash
uv tool install -p 3.12 git+ssh://git@github.com/0x53504852/dactory@add_package_management_and_entrypoint
uvx dactory --help
```

#### I want to clone the repo to edit it:
With uv, nothing is required to use the package. Simply run for example:

```bash
git clone git@github.com:0x53504852/dactory.git
cd dactory
uv run dactory create --help
```

### Installation with pip

We recommend creating a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate
```
Then you can install this package in editable mode:
```bash
pip install -e .
```
Check that it's working with 
```bash
dactory create --help
```

## How it works

Dactory downloads one corpus of CommonCrawl and then performs the following steps:
1) Remove any warc entry that is not a "response"
2) Try to decode the warc html with utf-8, and if it fails, ask `resiliparse` to detect the correct encoding.
3) Extract the text of the html with `resiliparse`.
4) Guess the language and add a score representing the confidence.
5) Remove the warc if not in the list of languages selected or if the confidence in the language is too low.
6) Dedupe the text, on a per-paragraph basis, with a bloom filter, based on warcs seen so far.
7) Score the text with models trained for each language, filter out warcs with a low score.
8) Put the warcs in a `.jsonl.zstd`.

Along those steps, if at any point, the text of a warc entry is below a given number of characters (500 by default), the entry is filtered out.

Each entry in the final jsonl has the following keys:
```
text: str
date: str
url: str
language: str
language_score: float
warc-id: str
scores: dict[str, float]
group_idx: int
warc_file: str
record_idx: int
```

For a deeper dive on the dataset creation, feel free to look at the code or the blog post.

## Usage

The tool will loop over all the groups selected in the CommonCrawl corpus. Within each group, there is parralelization with multiprocessing with the `-w` option. Empirically, setting `-w` to a value above 32 doesn't give much speedup. 
While that can work nicely on a single machine with ~32 cores, it can take multiple days to create the dataset.
Multiple options are available to speed things up if you fall into one of those three cases:
1) You have many nodes available with a scheduler like slurm.
2) You have a machine with more than 32 cores
3) You want to skip some filters

In all cases, dactory will try to resume the work by looking at what was already written. Any warc file completely processed won't be downloaded again if you stop and restart the process. If this isn't what you want, you should delete the destination  files before restarting dactory.

### Speeding up the dataset creation with slurm

If you have access to slurm, you can speed up the dataset creation by running the command on different nodes. For example:

```bash
srun --ntasks=100 --cpus-per-task=33  --mem-per-cpu=1G bash -c 'uv run dactory create -q -w 32 -g $SLURM_PROCID /shared/directory/'
```
So 33 processes per task here with 100 tasks (there is 100 groups in a corpus).

### Speeding up the dataset creating with xargs

This requires a beefy machine (> 32 cpus).
As an example, we start 10 processes with xargs and each process will start 8 workers. So 90 processes total. 90 cpus will be used. This command will download all groups.
```bash
seq 0 100 | xargs -P 10 -I {} uv run dactory create -q -w 8 -g {} /shared/directory/
```

### Speeding up the dataset creation by skipping some processing
Skipping some processing will drastically reduce the amount of cpu used. The most expensive processing operation is the scoring.
```bash
uv run dactory create \
  --lang-detection-model None \ # skips the language detection
  --bloom-filter None \         # skips the bloom filter
  --scoring-models None \       # skips the scoring of the documents
  dest/directory/
```

## Working/iterating on the codebase
### With uv

When using uv, use `uv run dactory create ...` and the rust code will be automatically recompiled if needed.

### With pip

If you change the Rust code, Rust dependencies or the Python dependencies you'll need to re-run `pip install -e .`.
Then call the command line interface with `dactory create ...`.

### Pre-commit
We recommend installing the pre-commit:
```bash
uv run pre-commit install
```



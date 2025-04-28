import line_profiler

profile = line_profiler.LineProfiler()

# Add @profile to any function in the codebase, and run any command, 
# you'll see a profiling report once the command is finished (even if it is stopped).
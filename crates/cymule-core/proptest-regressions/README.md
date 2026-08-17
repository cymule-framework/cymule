# Proptest Regression Corpus

`semantic_kernel.txt` is created when the semantic property suite finds and
minimizes a failing seed. Commit that generated file with the fix so focused,
soak, mutation, and CI runs replay the known case before generating new input.

Do not hand-edit or discard a persisted case merely because later generated
cases pass. Remove one only when the property no longer exists or the recorded
strategy is intentionally replaced, and explain that change in the commit.

import numpy as np

def local_alignment_professor(seq1, seq2, match=4, mismatch=-2, gap_open=-1, gap_extend=-1):
    n, m = len(seq1), len(seq2)
    H = np.zeros((n+1, m+1), dtype=int)

    for i in range(1, n+1):
        for j in range(1, m+1):
            # substitution
            score_sub = match if seq1[i-1] == seq2[j-1] else mismatch
            diag = H[i-1][j-1] + score_sub

            # horizontal gaps (jump left by k)
            left_scores = [H[i][j-k] + gap_open + (k-1)*gap_extend for k in range(1, j+1)]
            best_left = max(left_scores)

            # vertical gaps (jump up by k)
            up_scores = [H[i-k][j] + gap_open + (k-1)*gap_extend for k in range(1, i+1)]
            best_up = max(up_scores)

            # local alignment reset
            H[i][j] = max(0, diag, best_left, best_up)

    return H

# Sequences from screenshot
seq_vertical = "TAGATTCA T".replace(" ", "")   # rows (left side)
seq_horizontal = "TATCATAGGT"                  # cols (top side)

H = local_alignment_professor(seq_vertical, seq_horizontal)

print("DP Matrix:")
print(H)

# Important cells
print("\nPink =", H[2][2])  # should be 16
print("Gray =", H[2][1])   # should be 16
print("a =", H[4][4])      # should be 10
print("b =", H[4][5])      # should be 14
print("c =", H[4][6])      # should be 3)

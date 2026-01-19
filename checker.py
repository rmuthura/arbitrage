def print_stakes_and_profit(odds_a, odds_b, bankroll):
    def to_decimal(odds):
        return 1 + (odds/100) if odds > 0 else 1 + (100/(-odds))

    # implied probabilities (for arb check)
    def to_prob(odds):
        return 100/(odds+100) if odds > 0 else (-odds)/(-odds+100)

    p_sum = to_prob(odds_a) + to_prob(odds_b)
    if p_sum >= 1.0:
        print("No arbitrage (sum of implied probs >= 1).")
        return

    d_a, d_b = to_decimal(odds_a), to_decimal(odds_b)
    P = bankroll / (1/d_a + 1/d_b)      # equalized payout
    stake_a = P / d_a
    stake_b = P / d_b
    profit = P - bankroll

    print(f"Stake @ {odds_a:+}: ${stake_a:.2f}")
    print(f"Stake @ {odds_b:+}: ${stake_b:.2f}")
    print(f"Guaranteed profit: ${profit:.2f}")

# Example:
print_stakes_and_profit(+235, -225, 100)

import numpy as np

def psi(expected, actual, buckets=10):
    def safe_scale(arr):
        arr = np.array(arr)
        if arr.max() == arr.min():
            return np.zeros_like(arr)
        return np.interp(arr, (arr.min(), arr.max()), (1, 100))

    expected = safe_scale(expected)
    actual = safe_scale(actual)

    bins = np.linspace(0, 100, buckets + 1)
    psi_value = 0

    for i in range(buckets):
        e = ((expected >= bins[i]) & (expected < bins[i+1])).mean()
        a = ((actual >= bins[i]) & (actual < bins[i+1])).mean()
        e = max(e, 1e-6)
        a = max(a, 1e-6)
        psi_value += (e - a) * np.log(e / a)

    return psi_value

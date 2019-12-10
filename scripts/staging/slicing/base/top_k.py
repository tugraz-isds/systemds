class Topk:
    k: int
    min_score: float
    slices: []

    def __init__(self, k):
        self.k = k
        self.slices = []
        self.min_score = 1

    def top_k_min_score(self):
        self.slices.sort(key=lambda x: x.score, reverse=True)
        self.min_score = self.slices[len(self.slices) - 1].score
        return self.min_score

    def add_new_top_slice(self, new_top_slice):
        self.min_score = new_top_slice.score
        if len(self.slices) < self.k:
            self.slices.append(new_top_slice)
            return self.top_k_min_score()
        else:
            self.slices[len(self.slices) - 1] = new_top_slice
            return self.top_k_min_score()

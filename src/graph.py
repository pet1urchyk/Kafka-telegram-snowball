import random

SMALL = {
    1: [2, 3],
    2: [3, 4],
    3: [1, 5],
    4: [5],
    5: [2],
}


def make_graph(n=200, seed=42, max_children=4):
    """Великий детермінований граф: одне й те саме при кожному запуску."""
    rng = random.Random(seed)
    return {
        v: [rng.randint(1, n) for _ in range(rng.randint(0, max_children))]
        for v in range(1, n + 1)
    }


GRAPH = SMALL            # для великого тесту заміниш на make_graph()


def children(channel_id):
    return GRAPH.get(channel_id, [])
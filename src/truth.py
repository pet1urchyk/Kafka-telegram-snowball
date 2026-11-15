from collections import deque

from graph import children


def truth(start=1, max_depth=10):
    depth_of = {start: 0}       
    queue = deque([start])
    found_messages = 0

    while queue:
        v = queue.popleft()
        d = depth_of[v]
        kids = children(v)
        found_messages += len(kids)            

        for k in kids:
            if k not in depth_of and d + 1 <= max_depth:    
                depth_of[k] = d + 1          
                queue.append(k)

    return depth_of, found_messages


if __name__ == "__main__":
    depth_of, msgs = truth(start=1, max_depth=2)
    print("унікальних seed-ів:", len(depth_of), sorted(depth_of))
    print("повідомлень у found:", msgs)
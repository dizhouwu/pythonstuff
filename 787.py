#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# DP
class Solution:
    def findCheapestPrice(self, n: int, flights: List[List[int]], src: int, dst: int, K: int) -> int:
        
        dp = [float('inf')/2]*n
        dp[src] = 0
        
      # dp[j]: shortest distance between src and j  
        for k in range(K+1):
            
                dp_tmp = dp.copy()
                for flight in flights:
                    a = flight[0]
                    b = flight[1]
                    w = flight[2]
                    
                    dp[b] = min(dp[b], w+dp_tmp[a])
        
        if (dp[dst] >= float('inf')/2):
            return -1
        else:
            return dp[dst]


# In[ ]:


# priority queue
class Solution:
    def findCheapestPrice(self, n: int, flights: List[List[int]], src: int, dst: int, K: int) -> int:
        
        from collections import defaultdict
        import heapq
        # price dict
        p = defaultdict(dict)
        
        for start, end, price in flights:
            p[start][end] = price
        
        heap = [(0, src, K+1)]
        
        while heap:
            price, src, dist = heapq.heappop(heap)
            if src == dst:
                return price
            if dist >0:
                for end in p[src]:
                    heapq.heappush(heap, (p[src][end]+price, end, dist-1))
        return -1


# In[ ]:


# DFS
class Solution:
    def findCheapestPrice(self, n: int, flights: List[List[int]], src: int, dst: int, K: int) -> int:
        
        from collections import defaultdict
        p = defaultdict(dict)
        
        for start, end, price in flights:
            p[start][end] = price
        
        visited = [0]*n
        ans = [float('inf')]
        
        self._dfs(p, src, dst, K+1, 0, visited, ans)
        return -1 if ans[0] == float('inf') else ans[0]
        

    def _dfs(self, p, src, dst, K, cost, visited, ans):
        
        if src == dst:
            ans[0] = cost
            return 
        
        if K == 0: return
        
        for end, price in p[src].items():
            if visited[end]:
                continue
            if cost + price > ans[0]:
                continue
            visited[end] = 1
            self._dfs(p, end, dst, K-1, cost+price,visited, ans)
            visited[end] = 0


# In[ ]:


# BFS
class Solution:
    def findCheapestPrice(self, n: int, flights: List[List[int]], src: int, dst: int, K: int) -> int:
        
        from collections import defaultdict, deque
        p = defaultdict(dict)
        
        for start, end, price in flights:
            p[start][end] = price
        ans = float('inf')
        que = collections.deque()
        que.append((src, 0))
        step = 0
        while que:
            size = len(que)
            for i in range(size):
                cur, cost = que.popleft()
                if cur == dst:
                    ans = min(ans, cost)
                for end, price in p[cur].items():
                    if cost + price > ans:
                        continue
                    que.append((end, cost + price))
            if step > K: break
            step += 1
        return -1 if ans == float('inf') else ans


# In[ ]:





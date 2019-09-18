#!/usr/bin/env python
# coding: utf-8

# In[64]:


import random

def montecarlo(experiment, trials):
    r = 0
    for i in range(trials):
        experiment.reset()
        r += 1 if experiment.run() else 0
    return r / np.float64(trials)


class Coinrowflipper():
    def __init__(self, trials, headsinrow):
        self.headsinrow = headsinrow
        self.trials = trials
        self.reset()
    def reset(self):
        self.count = 0
    def run(self):
        for i in range(self.trials):
            if random.random() < 0.5:
                self.count += 1
                if self.count > self.headsinrow:
                    return False
            else:
                self.count = 0
        return True

c = Coinrowflipper(1000, 6)
montecarlo(c, 100000)


# In[ ]:





# In[ ]:





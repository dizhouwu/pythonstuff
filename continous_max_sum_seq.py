#!/usr/bin/env python
# coding: utf-8

# 第一个是continuous max sum of a sequence
# 第二个是matrix from top left to botton right, what's the path that has minimum sum

# In[11]:


from sys import maxsize

def maxSubArraySum(a,size): 
       
    max_so_far = -maxsize - 1
    max_ending_here = 0
       
    for i in range(0, size): 
        max_ending_here = max_ending_here + a[i] 
        if (max_so_far < max_ending_here): 
            max_so_far = max_ending_here 
  
        if max_ending_here < 0: 
            max_ending_here = 0   
    return max_so_far 
   
# Driver function to check the above function  
a = [-13, -3, -25, -20, -3, -16, -23, -12, -5, -22, -15, -4, -7] 
maxSubArraySum(a,len(a)) 
   


# In[12]:


b = [-2,-3,4,-1,-2,1,5,-3]


# In[13]:


maxSubArraySum(b, len(b))


# In[ ]:





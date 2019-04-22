import numpy as np
import random
import scipy.stats.mstats
 
VaRprob = 0.05 # VaR probability level: 0.05 means 5% of the time VaR level is exceeded
 
# globals for get_data batch transform decorator
R_P = 0  # refresh period
window = 100 # window length - Also the Historical VaR window length
 
 
def initialize(context):
    # 14 large cap stocks
    context.stocks = [sid(24),sid(26578),sid(3149), sid(5061), sid(3766),sid(23112),sid(4151),sid(5938),sid(5923),sid(6653),sid(700), sid(1900), sid(8347), sid(8229)]
    context.day = 0
    context.VaRBigLossVec = np.zeros(window+1)
    
def handle_data(context, data): 
    all_prices = get_data(data, context.stocks)
    nstocks = len(context.stocks)    
    if all_prices is None: 
        return
    daily_returns = get_returns(all_prices,window, nstocks)
    
    # Buy equal amounts of each stock on the first day
    stock = 0
    if context.day == 1:
      portfolio_value = context.portfolio.cash
      for security in context.stocks:
          price = all_prices[stock][window-1]
          NosharesEquallyWeighted = (portfolio_value)/(price * nstocks )
          order(security,NosharesEquallyWeighted)          
          stock = stock +1
 
    # Increment context.day
    context.day = context.day+1        
    # Need context.day to be bigger than window to calculate VaR
    if context.day < window:  
        return
    
    # Calculate VaR
    VaR=calculateVaR(daily_returns,window,nstocks,VaRprob,context)
    print "Value at risk is $" + str(VaR[0])
    
    # Yesterdays portfolio value
    prev_port_val = 0
    stock = 0
    for security in context.stocks:
        prev_position_value = context.portfolio.positions[security].amount * all_prices[stock][window-2]
        prev_port_val += prev_position_value
        stock = stock +1
    
    #  Current portfolio value
    portfolio_value = context.portfolio.positions_value  
    dport_val= portfolio_value-prev_port_val # Change in port value
    print "Change in Portfolio Value $" + str(dport_val)
    
    # See if portfolio loss was greater than VaR
    if dport_val < -VaR[0]:
        BigLoss = 1
    else:
        BigLoss = 0  
    # Append this to a vector, to compare predicted VaR exceptions to actual VaR exceptions    
    context.VaRBigLossVec = np.append( context.VaRBigLossVec, BigLoss )
    
    # Count the number of VaR exceptions in the last window amount of days
    VarExceptions = 0
    for jj in range(len(context.VaRBigLossVec)-1-window, len(context.VaRBigLossVec)-1 ): 
            VarExceptions += context.VaRBigLossVec[jj]/window
    print "Actual Var Exceptions probability: " + str(VarExceptions)
    
    ##########################    
 
    ### View 1: Predicted VaR vs change in portfolio value
    ### The  change in portfolio value should break through VaR, VaRprob % of the time
    record(PredictedVaR = -VaR[0], ChangeinPortfolioValue = dport_val)
    
    
    ### View 2: Compare predicted VaR exceptions to expected exceptions - make sure you uncomment all 3 lines 
    ### Note ExceedVaREvent is an indicator function: 0.1 for VaR exception at that time, 0 otherwise
    #if context.day < 2*window:  # Need to build up statistics 
    #    return     
    #record( ActualVarExceptions = VarExceptions, PredictedVarExceptions = VaRprob ,  ExceedVaREvent = BigLoss*0.01 ) 
    
 
 
        
########################################################################################
# Modules
########################################################################################
 
@batch_transform(refresh_period=R_P, window_length=window) # set globals R_P & window above
def get_data(datapanel,sids):
    return datapanel['price'].as_matrix(sids).transpose()
 
 
def get_returns(all_prices,window,nstocks):
    daily_returns = np.zeros((window,nstocks))
    for stock in range(0,nstocks):
        for day in range(0,window):
            daily_returns[day][stock] = (all_prices[stock][day]-all_prices[stock][day-1])/all_prices[stock][day-1]    
    return daily_returns
 
 
 
def calculateVaR(daily_returns,window,nstocks,VaRprob,context):
    # Initialize
    weight = np.zeros(nstocks)
    PnL = np.zeros(window)
    
    portfolio_value = context.portfolio.positions_value  # As long only here.
    #Calculate portfolio weights
    stock = 0
    for security in context.stocks:
        current_position_value = context.portfolio.positions[security].amount * context.portfolio.positions[security].last_sale_price
        weight[stock] = current_position_value/portfolio_value
        stock = stock +1
        
    # Find Profit and loss distribution 
    for day in range(0,window-1): 
        for stock in range(0,nstocks):
            PnL[day] += portfolio_value*weight[stock]*daily_returns[day][stock]
            
    # Find quantile of PnL
    PnLQuantile = scipy.stats.mstats.mquantiles( PnL, VaRprob  )
    PnLMean = np.average(PnL)
    # VaR relative to the mean
    VaR = PnLMean - PnLQuantile
    return VaR
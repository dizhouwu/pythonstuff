#Reference: https://cloud.tencent.com/developer/article/1386452


#------------------------------schmidt_orthogonalization----------------------------------
def schmidt(factors: pd.DataFrame,order: list)->pd.DataFrame:
    '''
    Perform schmidt_orthogonalization on a Factor dataframe
   
   :param: factors: pd.DataFrame: a N*K dataframe of the factor value, N stocks,k factors
   :param: order: list, can be fixed order or dynamic order
   
   Return: 
   
   factor_new: pd.DataFrame: the factors dataframe after orthogonization
    '''
    factors=factors.copy()
    factors_raw = factors.copy()
    # drop na
    factors = factors.loc[factors.notnull().all(axis=1), :]
    # centralization
    factors = factors.apply(lambda x:x-x.mean(),axis=0)
    factors=factors.loc[:,order]
    factor_result=np.zeros_like(factors)
    factors_value=factors.values
    for i in range(factors_value.shape[1]):
        y=factors_value[:,i]
        x=factors_value[:,:i]
        if x.shape[1]>0:
            coef=np.linalg.lstsq(x, y)[0]
            pred=np.array((np.mat(x)*np.mat(coef).T).T)[0]
            resi=y-pred
        else:
            resi=y
        resi=(resi-resi.mean())/resi.std()
        factor_result[:,i]=resi
        
    factor_new=pd.DataFrame(data=factor_result,index=factors.index,columns=factors.columns)
    factor_new=factor_new.reindex(index=factors_raw.index,columns=factors_raw.columns)
    
    return factor_new

#------------------------------schmidt_orthogonalization----------------------------------

def canonial(factors: pd.DataFrame)->pd.DataFrame:
    '''
    Perform canonical orthogonization
    
   :param: factors: pd.DataFrame: a N*K dataframe of the factor value, N stocks,k factors
   
   Return:
   
   f_hat: pd.DataFrame: the factors dataframe after orthogonization
    '''
    factors = factors.loc[factors.notnull().all(axis=1), :]
    # normalization
    factors = factors.apply(lambda x:(x-x.mean()/x.std()),axis=0)
    col_name = factors.columns
    ,D, U = np.linalg.eig(np.dot(factors.T, factors))
    S = np.dot(U, np.diag(D**(-0.5)))
    
    f_hat = np.dot(factors, S)
    f_hat = pd.DataFrame(f_hat, columns=col_name, index=factors.index)
    
    return f_hat
    
#------------------------------symmetry_orthogonalization----------------------------------
def symmetry(factors, pd.DataFrame)->pd.DataFrame:
   '''
    Perform symmetry orthogonization
    
   :param: factors: pd.DataFrame: a N*K dataframe of the factor value, N stocks,k factors
   
   Return:
   
   f_hat: pd.DataFrame: the factors dataframe after orthogonization
    '''
    col_name = factors.columns     
    D,U=np.linalg.eig(np.dot(factors.T,factors))
    S = np.dot(U,np.diag(D**(-0.5)))
    
    f_hat = np.dot(factors,S)
    f_hat = np.dot(f_hat,U.T)
    f_hat = pd.DataFrame(f_hat,columns = col_name,index = factors.index)
    
    return f_hat
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
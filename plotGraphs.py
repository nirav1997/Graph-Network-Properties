#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 20:24:51 2020

@author: shaivalshah
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#import scipy.stats.powerlaw
files = ['gnp2']
for file in files:
    print("------------------------------{}------------------------------".format(file))
    df = pd.read_csv('stanford_graphs/amazon.graph.small.csv', header=None)
    df = df.drop(df.columns[0],axis=1)
    x = np.array(df[df.columns[0]].to_list())
    y = np.array(df[df.columns[1]].to_list())
    x_new = np.linspace(x[1],x[-1], 100)
    
    ccdf = y.astype(float) / y.max()
    
    # Fit a line in log-space
    logx = np.log(x)
    logy = np.log(ccdf)
    params = np.polyfit(logx, logy, 1)
    est = np.exp(np.polyval(params, logx))
    
    fig, ax = plt.subplots()
    ax.loglog(x, ccdf, color='lightblue', ls='', marker='o',
              clip_on=False, zorder=10, label='Observations')
    
    ax.plot(x, est, color='salmon', label='Fit', ls='--')
    
    ax.set(xlabel='Reputation', title='CCDF of Stackoverflow Reputation',
           ylabel='Probability that Reputation is Greater than X')
    
    plt.show()
    
    params = np.polyfit(x,y,1)
    f, ax = plt.subplots(figsize=(5, 5))
    #ax.set(xscale="log", yscale="log")
    ax.scatter(x, y)
    
    ax.set_title('Distribution')
    ax.set_xlabel(df.columns[0])
    ax.set_ylabel(df.columns[1])
    ax.plot(params)
    print("------------------------------------------------------------------")
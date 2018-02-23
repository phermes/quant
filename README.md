# Q-AnT
## Installation
### Prerequisites


```python
conda install plotly
conda install -c anaconda pandas-datareader
conda install -c anaconda beautifulsoup4 
```

## Usage
Advanced examples are shown in `manual.ipynb`. The Q-AnT module can be loaded by importing the basic classes stock and Index from Q-AnT:
```python
from Q-Ant import stock, Index

```
   
   
   
## Variables

```

    stock.mean_return : the mean daily return of this stock based on
        historic price information

    stock.volatility : the standard deviation of the daily return based on historic
        price information
        
```   
   


## Installation

   conda install plotly
   conda install -c anaconda pandas-datareader
   conda install -c anaconda beautifulsoup4 


## Further implementation strategy

- Monitor the expected income of each stock to identifiy if it was revised
- Complete the managing of index quotes
- New class for currencies
  -Download/update 
- Classify stocks by sectors
- Save all transactions in a database (like blockchain)
- New class portfolio
  - check portfolio balance

## Stocks with missing data
- ES0178430E18 Telefonica
- CH0267291224 Sunrise
- GLAXOSMITHKLINE   LS-,25
- FREENET
- BLACKROCK

- Implement class for currencies
- Classify stocks by sectors
- Save all transactions in a database (like blockchain)
- Weight attractiveness of new stocks denpending on if the sector is already represented in the portfolio   


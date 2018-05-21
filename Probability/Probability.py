# Authors : Pascal Hermes - Tom Mertens
# Version : 1.0
# Last modified : 6-5-2018

from collections import defaultdict
import sympy as S
from sympy.stats import E, Die
from sympy import stats
import numpy as np


def sum_of_dice_invdict():
    """
    Creates a dictionary where the keys are sum of a double dice roll and the values are the the dice combinations
    leading to this sum
    :return:
    """
    d = {(i, j): i + j for i in range(1, 7) for j in range(1, 7)}
    dinv = defaultdict(list)
    for i, j in d.items():
        dinv[j].append(i)
    return dinv


def print_sum_dice_prob():
    dinv = sum_of_dice_invdict()
    X = {i: len(j) / 36. for i, j in dinv.items()}
    print(X)


def sum_dice_product_larger_than_sum():
    d = {(i, j, k): ((i * j * k) / 2 > i + j + k) for i in range(1, 7) for j in range(1, 7) for k in range(1, 7)}
    dinv = defaultdict(list)
    for i, j in d.items():
        dinv[j].append(i)
    X = {i: len(j) / 6.0 ** 3 for i, j in dinv.items()}
    print(X)


if __name__ == '__main__':
    diceinv = sum_of_dice_invdict()
    print(diceinv[7])
    print_sum_dice_prob()
    sum_dice_product_larger_than_sum()

    x = Die('D1', 6)
    y = Die('D2', 6)

    a = S.symbols('a')
    z = x + y
    J = E((x - a * (x + y)) ** 2)
    print(S.simplify(J))
    sol, = S.solve(S.diff(J, a), a)
    print(sol)

    samples_z7 = lambda: stats.sample(x, S.Eq(z, 7))
    mn0 = np.mean([(6. - samples_z7()) ** 2 for i in range(100)])
    mn = np.mean([(7 / 2. - samples_z7()) ** 2 for i in range(100)])
    print('MSE={:6.2f} using 6 vs MSE= {:6.2f} using 7/2.'.format(mn0, mn))

    # binomial distribution
    p,t = S.symbols('p t',positive=True)
    x= stats.Binomial('x',10,p)
    mgf = stats.E(S.exp(t*x))

    # mean using integral method
    print(S.simplify(stats.E(x)))
    # using moment generating functions
    print(S.simplify(S.diff(mgf,t).subs(t,0)))

    print(S.simplify(stats.moment(x,1)))
    print(S.simplify(stats.moment(x,2)))

    # two normally distributed random variables
    S.var('x:2',real=True)
    S.var('mu:2',real = True)

    S.var('sigma:2',positive=True)
    S.var('t',positive=True)

    x0 = stats.Normal(x0,mu0,sigma0)
    x1 = stats.Normal(x1,mu1,sigma1)

    mgf0 = S.simplify(stats.E(S.exp(t*x0)))
    mgf1 = S.simplify(stats.E(S.exp(t*x1)))
    mgfY = S.simplify(mgf0*mgf1)

    print(S.collect(S.expand(S.log(mgfY)),t))

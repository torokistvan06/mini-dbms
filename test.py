from pytest import fixture
from sympy import Function


xx = [1, 2, 3]
fx  = Function('fx')
fxx = fx(xx)

print(xx)
print(fxx)

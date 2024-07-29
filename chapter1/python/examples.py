#!/usr/bin/env python3

# MIT License
#
# Copyright (c) 2021 Packt
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# random record batch generator

import pyarrow as pa
import numpy as np
NROWS = 8192
NCOLS = 16
data = [pa.array(np.random.randn(NROWS)) for i in range(NCOLS)]
cols = ['c' + str(i) for i in range(NCOLS)] 
rb = pa.RecordBatch.from_arrays(data, cols)
print(rb.schema)
print(rb.num_rows)

# building a struct array

# One define a regular Python list to start from
archer_list = [{
    'archer': 'Legolas',
    'location': 'Murkwood',
    'year': 1954,
},{
    'archer': 'Oliver',
    'location': 'Star City',
    'year': 1941,
},{
    'archer': 'Merida',
    'location': 'Scotland',
    'year': 2012,
},{
    'archer': 'Lara',
    'location': 'London',
    'year': 1996,
},{
    'archer': 'Artemis',
    'location': 'Greece',
    'year': -600,
}]

# And one schema for Data
archer_type = pa.struct([('archer', pa.utf8()),
                         ('location', pa.utf8()),
                         ('year', pa.int16())])
# And now we create one Pyarrow Array from a regular Python list
# From all all operations in PyArrow objects goes in zero-copy mechanism.
archers = pa.array(archer_list, type=archer_type)
print(archers.type)
print(archers)
# O método flatten() da classe pyarrow.lib.StructArray converte da representação
# estruturada para uma representação em matriz, ou seja, representação FLAT.
# Os dados tabulares são convertidos para uma matriz bidimensional.
rb = pa.RecordBatch.from_arrays(archers.flatten(), 
                                ['archer', 'location', 'year'])
print(rb)
"""
Veja abaixo a representação String do objeto rb, como é impresso.

pyarrow.RecordBatch
archer: string
location: string
year: int16
----
archer: ["Legolas","Oliver","Merida","Lara","Artemis"]
location: ["Murkwood","Star City","Scotland","London","Greece"]
year: [1954,1941,2012,1996,-600]
"""

"""
This is a zero-copy manipulation

The record batch we created holds references to the exact same
arrays we created for the struct array, not copies, which makes
this a very efficient operation, even for very large data sets.
Cleaning, restructuring, and manipulating raw data into a more
understandable or easier to work with format is a common task
for data scientists and engineers. One of the strengths of using
Arrow is that this can be done efficiently and without making
copies of the data.
"""
print(rb.num_rows) # prints 5
print(rb.num_columns) # prints 3

# slices
"""
More zero-copy manipulation

Another common situation when working with data is when you only
need a particular slice of your dataset to work on, rather than
the entire thing. As before, the library provides a slice function
for slicing record batches or arrays without copying memory. 
Think back to the structure of the arrays; because any array has
a length, null count, and sequence of buffers, the buffers that are
used for a given array can be slices of the buffers from a larger array.
This allows working with subsets of data without having to copy it around.
"""
slice = rb.slice(1, 3) # (start, length)
print(slice.num_rows) # prints 3, not 5
print(rb.column(0)[0]) # <pyarrow.StringScalar: 'Legolas'>
print(slice.column(0)[0]) # <pyarrow.StringScalar: 'Oliver'>
archerslice = archers[1:3] # slice of length 2 viewing indexes 
                           # 1 and 2 from the struct array
                           # so it slices all three arrays

"""
The costs to copy and convert the data

One thing that does make a copy though is to convert Arrow arrays back to
native Python objects for use with any other Python code that isn't using
Arrow. Just like I mentioned back at the beginning of this chapter, shifting
between different formats instead of the libraries all using the same one
has costs to copy and convert the data:
"""                           
print(rb.to_pydict()) # prints dict {column: list<values>}
print(archers.to_pylist()) # prints the same list of dictionaries
                           # we started with
"""
Both of the preceding calls, to_pylist and to_pydict, perform copies of the
data in order to put them into native Python object formats and should be
used sparingly with large datasets.
"""
print("Fim")

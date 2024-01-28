
# from __future__ import prunsigned long_function
from cpython cimport array
import array
cimport cython
from libc.stdlib cimport malloc, free
from cython.parallel import prange
from cython.parallel cimport prange
# from libc.math import log10
from libc.math cimport log10
# from libc.stdio cimport prunsigned longf



@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef list radix(list lst):
    cdef array.array arr = array.array("L", lst)
    processor(arr.data.as_ulongs, len(arr))
    return list(arr)



@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cdef unsigned long * processor(unsigned long *arr_view, unsigned long array_length):

    cdef unsigned long i
    cdef unsigned long exponent
    cdef unsigned long number_of_digits
    cdef unsigned long current_index
    cdef unsigned long current_element
    cdef unsigned long start
    cdef unsigned long stop
    
    
    cdef unsigned long position_within_buckets
    cdef unsigned long output_array_position
    cdef unsigned long count_of_digits
    
    cdef unsigned long max_length = get_max_element_length(arr_view, array_length)
    
    
    cdef unsigned long *indexes = <unsigned long *> malloc(10 * sizeof(unsigned long))
    cdef unsigned long *num_indexes = <unsigned long *> malloc(max_length * sizeof(unsigned long))
    cdef unsigned long *sorted_view = <unsigned long *> malloc(array_length*max_length * sizeof(unsigned long))
    

    
    # set each beginning index according to the number of elements maximally possible
    for i in range(max_length):
        num_indexes[i] = i*array_length
    # iterate through array
    # count number of digits (while loop) and place unsigned longo according chunks
    for i in range(array_length):
        exponent = 10
        current_element = arr_view[i]
        number_of_digits = <unsigned long>log10(current_element)
        sorted_view[num_indexes[number_of_digits]] = current_element
        num_indexes[number_of_digits] += 1

    # process number_of_digit chunks and write them unsigned longo output array
    with nogil:
        for count_of_digits in prange(max_length, schedule='static'):            
            start = array_length*count_of_digits
            stop = num_indexes[count_of_digits]
            radix_int(sorted_view, start, stop, indexes, count_of_digits+1, 
                      10, 1, stop - start, array_length)

    output_array_position = 0
    for count_of_digits in range(max_length): 
        start = array_length*count_of_digits
        stop = num_indexes[count_of_digits]
        for position_within_buckets in range(start, stop):
            arr_view[output_array_position] = sorted_view[position_within_buckets]
            output_array_position += 1
    free(indexes)
    free(num_indexes)
    free(sorted_view)
    # return output


@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cdef unsigned long * radix_int(unsigned long *unsorted, unsigned long start, unsigned long stop, unsigned long *indexes, unsigned long max_length, 
                     unsigned long p, unsigned long p10, unsigned long length, unsigned long array_length) nogil:
    cdef unsigned long j
    cdef unsigned long i
    cdef unsigned long *buckets = <unsigned long *> malloc(length*10 * sizeof(unsigned long))

    for j in range(max_length):
        for_loop(unsorted, start, stop, indexes, p, p10, length, buckets)
        p10 = p
        p *= 10

    free(buckets)

@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cdef unsigned long * for_loop(unsigned long *unsorted_data, unsigned long data_start, unsigned long data_stop, unsigned long *indexes, unsigned long p, unsigned long p10, unsigned long length, unsigned long *buckets) nogil:
    cdef unsigned long new_element_index
    cdef unsigned long i
    cdef unsigned long start
    cdef unsigned long k
    cdef unsigned long data_idx
    cdef unsigned long k_length

    cdef unsigned long k_length_data_start

    for k in range(10):
        indexes[k] = length*k
    for i in range(data_start, data_stop):
        new_element_index = (unsorted_data[i] % p) // p10
        buckets[indexes[new_element_index]] = unsorted_data[i]
        indexes[new_element_index] += 1

    data_idx = data_start
    for k in range(10):
        start = indexes[k]
        k_length = length*k
        for i in range(k_length, start):
            unsorted_data[data_idx] = buckets[i]
            data_idx += 1
    return unsorted_data

@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cdef unsigned long get_max_element_length(unsigned long *arr, unsigned long length):
    cdef unsigned long i = 0
    cdef unsigned long p = 1
    cdef unsigned long current_max = arr[0]
    for i in range(length):
        if arr[i] > current_max:
            current_max = arr[i]
    i = 0
    while current_max // p != 0:
        i += 1
        p *= 10
    return i

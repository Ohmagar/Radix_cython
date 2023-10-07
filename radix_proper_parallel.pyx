
from __future__ import print_function
from cpython cimport array
import array
cimport cython
from libc.stdlib cimport malloc, free
from cython.parallel import prange
from cython.parallel cimport prange
from libc.stdio cimport printf
from libc.math cimport pow, floor



@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef list radix(list lst):
    cdef array.array arr = array.array("i", lst)
    cdef array.array output = processor(arr)
    return list(output)



@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cdef array.array processor(array.array arr):
    cdef array.array output_template = array.array("i", [])
    
    cdef int[:] arr_view = arr
    
    cdef int i
    cdef int exponent
    cdef int number_of_digits
    cdef int current_index
    cdef int current_element
    cdef int start
    cdef int stop
    
    
    cdef Py_ssize_t array_length = len(arr) 
    cdef Py_ssize_t position_within_buckets
    cdef Py_ssize_t output_array_position
    cdef Py_ssize_t count_of_digits
    
    cdef int max_length = get_max_element_length(arr_view, array_length)
    
    cdef array.array output = array.clone(output_template, array_length, zero=True)

    
    cdef Py_ssize_t *indexes = <Py_ssize_t *> malloc(10 * sizeof(Py_ssize_t))
    cdef Py_ssize_t *num_indexes = <Py_ssize_t *> malloc(max_length * sizeof(Py_ssize_t))
    cdef int *sorted_view = <int *> malloc(array_length*max_length * sizeof(int))
    

    cdef array.array printer = array.clone(output_template, array_length, zero=True)
    cdef int[:] output_view = output
        
    cdef int idx = 0
    
    
    # set each beginning index according to the number of elements maximally possible
    # with nogil:
    for i in range(max_length):
        num_indexes[i] = i*array_length
    # iterate through array
    # count number of digits (while loop) and place into according chunks
    for i in range(array_length):
        exponent = 10
        current_element = arr_view[i]
        for number_of_digits in range(max_length):
            if current_element < exponent:
                break
            exponent *= 10
        sorted_view[num_indexes[number_of_digits]] = current_element
        num_indexes[number_of_digits] += 1


    for number_of_digits in range(max_length):  
        start = array_length*number_of_digits
        stop = num_indexes[number_of_digits]
        idx = pre_sort_digits(sorted_view, start, stop, number_of_digits+1, stop-start, idx)

    for output_array_position in range(array_length):
        output_view[output_array_position] = sorted_view[output_array_position]

    free(indexes)
    free(num_indexes)
    free(sorted_view)
    return output



@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cdef int * radix_int(int *unsorted, Py_ssize_t start, Py_ssize_t stop, int max_length, 
                     size_t p, size_t p10, size_t length, Py_ssize_t array_length) nogil:
    cdef int j
    cdef Py_ssize_t *buckets = <Py_ssize_t *> malloc(length*10 * sizeof(Py_ssize_t))
    cdef Py_ssize_t *indexes = <Py_ssize_t *> malloc(10 * sizeof(Py_ssize_t))

    for j in range(max_length):
        for_loop(unsorted, start, stop, indexes, p, p10, length, buckets)
        p10 = p
        p *= 10

    free(buckets)
    free(indexes)

@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cdef int * for_loop(int *unsorted_data, size_t data_start, size_t data_stop, Py_ssize_t *indexes, size_t p, size_t p10, size_t length, Py_ssize_t *buckets) nogil:
    cdef size_t new_element_index
    cdef size_t i
    cdef size_t start
    cdef size_t k
    cdef size_t data_idx
    cdef size_t k_length

    cdef size_t k_length_data_start
    

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
cdef int pre_sort_digits(int *unsorted_first_digits, ssize_t data_start, ssize_t data_stop, ssize_t number_of_digits, Py_ssize_t length, int idx):# nogil:
    cdef ssize_t flooring = 10 ** number_of_digits
    cdef int *sorted_first_digits = <int *> malloc(length * 10 * sizeof(int))
    cdef ssize_t *bucket_indexes = <ssize_t *> malloc(10 * sizeof(ssize_t))
    cdef ssize_t i
    cdef ssize_t first_digit
    cdef int current_element
    cdef ssize_t data_idx = 0
    
    cdef ssize_t start
    cdef ssize_t stop
    cdef ssize_t count_of_digits
    
    
    for i in range(10):
        bucket_indexes[i] = length * i

    for i in range(data_start, data_stop):
        current_element = unsorted_first_digits[i]
        first_digit = current_element // flooring
        sorted_first_digits[bucket_indexes[first_digit]] = current_element
        bucket_indexes[first_digit] += 1
    
    with nogil:
        for count_of_digits in prange(10, schedule='static'):            
            start = length * count_of_digits
            stop = bucket_indexes[count_of_digits]
            radix_int(sorted_first_digits, start, stop, number_of_digits, 10, 1, stop - start, length)

    for i in range(10):
        for current_element in range(length*i, bucket_indexes[i]):
            unsorted_first_digits[idx] = sorted_first_digits[current_element]
            idx += 1
    free(sorted_first_digits)
    free(bucket_indexes)
    return idx
    

@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cdef size_t get_max_element_length(int[:] arr, size_t length):
    cdef size_t i = 0
    cdef int p = 1
    cdef int current_max = arr[0]
    for i in range(length):
        if arr[i] > current_max:
            current_max = arr[i]
    i = 0
    while current_max // p != 0:
        i += 1
        p *= 10
    return i

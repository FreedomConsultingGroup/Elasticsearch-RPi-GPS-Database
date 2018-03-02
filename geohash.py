# Base 32 as defined by Douglass Crockford, also with 'A' omitted and 'U' included
# https://en.wikipedia.org/wiki/Base32#Crockford's_Base32
CROCKFORDBASE32_bin = {'0': '00000', '1': '00001', '2': '00010', '3': '00011', '4': '00100', '5': '00101',
                        '6': '00110', '7': '00111', '8': '01000', '9': '01001', 'b': '01010', 'c': '01011',
                        'd': '01100', 'e': '01101', 'f': '01110', 'g': '01111', 'h': '10000', 'j': '10001',
                        'k': '10010', 'm': '10011', 'n': '10100', 'p': '10101', 'q': '10110', 'r': '10111',
                        's': '11000', 't': '11001', 'u': '11010', 'v': '11011', 'w': '11100', 'x': '11101',
                        'y': '11110', 'z': '11111'}
CROCKFORDBASE32_alpha = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k',
                         'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')

"""Geohashing is a method used to compress latitude and longitude coordinates into base 32 based on the precision of the
 coordinates. For a detailed explanation with an example, see the Wikipedia article dedicated to it: 
                                                                                https://en.wikipedia.org/wiki/Geohash
 
 Basically, an "average" is selected from a minimum and maximum, beginning at (-90, 90) for latitude, and (-180, 180)
 for longitude, which both average to 0. Each even bit is dedicated to the longitude, and each odd bit is dedicated to
 the latitude.
 
     For example, if the geohash "ezs42" is given, we decode it to binary using the above dictionary,
     which results in the number "01101 11111 11000 00100 00010" this means that the numbers we use for the longitude
     and latitude are "0111110000000" (the even bits) and "101111001001" (the odd bits), respectively.
 
 We check the digit at each index. If it is "1", we take the upper half of the interval. If it is "0", we take the lower
 half. Then, repeat with the new interval.
 
     Taking the previous numbers for latitude and longitude. We start with longitude at the
     interval (-180, 180), which averages to 0. The bit at index 0 of the longitude is "0", so we take the lower half of
     our interval: (-180, 0). Then repeat with our new interval. The average of (-180, 0) is -90, and the bit at index
     1 is "1", so our new interval becomes (-90, 0), which is the upper half of the interval. We repeat this for every
     bit in the number, and then for the latitude as well.
 
 To encode numbers, we simply do the opposite of decoding. For even bits, if our longitude is greater than or equal to
 the average of our interval, we take the upper half of the interval. If it is less than the average, we take the lower
 half. Do the same thing for odd bits with the latitude, and repeat for the specified precision.
 
 The speed of the 'geohash' function is proportionate to the 'bin_precision' variable
 The speed of the 'ungeohash' function is proportionate to the length of the geohash in base 32 plus its length in binary
 
 Note that geohashing is a form of lossy compression. With enough precision, the loss is negligible. However, the
 +- error is returned with the hash, and should be kept with the hash if needed.
"""


def geohash(lat, lon, bin_precision=65):
    hashed_result = ''
    bin_result = ''
    lat_interval = [-90, 90]
    lat_error = 90
    lon_interval = [-180, 180]
    lon_error = 180

    for bit in range(bin_precision):
        if bit % 2 == 0:
            mid = (lon_interval[0] + lon_interval[1]) / 2
            if lon >= mid:
                bin_result += '1'
                lon_interval[0] = mid
                lon_error /= 2
            else:
                bin_result += '0'
                lon_interval[1] = mid
                lon_error /= 2
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2
            if lat >= mid:
                bin_result += '1'
                lat_interval[0] = mid
                lat_error /= 2
            else:
                bin_result += '0'
                lat_interval[1] = mid
                lat_error /= 2

        if len(bin_result) > 4:
            num_result = int(bin_result, 2)
            hashed_result += CROCKFORDBASE32_alpha[num_result]
            bin_result = ''
    if len(bin_result) > 0:
        num_result = int(bin_result, 2)
        hashed_result += CROCKFORDBASE32_alpha[num_result]
    return hashed_result, lat_error, lon_error


def ungeohash(s, dec_precision=6):
    bin_total = ''
    for digit in s:
        num = CROCKFORDBASE32_bin[digit]
        bin_total += num

    lat_interval = [-90, 90]
    lat_error = 45
    lon_interval = [-180, 180]
    lon_error = 90
    for bit_index in range(len(bin_total)):
        bit = bin_total[bit_index]
        if bit_index % 2 == 0:
            mid = sum(lon_interval) / 2
            if bit == '1':
                lon_interval[0] = mid
                lon_error /= 2
            else:
                lon_interval[1] = mid
                lon_error /= 2
        else:
            mid = sum(lat_interval) / 2
            if bit == '1':
                lat_interval[0] = mid
                lat_error /= 2
            else:
                lat_interval[1] = mid
                lat_error /= 2

    lat = sum(lat_interval)/2
    lon = sum(lon_interval)/2
    prec = pow(10, dec_precision)
    lat = round(lat*prec) / prec
    lon = round(lon*prec) / prec
    return lat, lat_error, lon, lon_error


# test function
if __name__ == '__main__':
    geo_hash = geohash(39.184038, -76.851789)  # dqcqgjnpxd0eq
    print(geo_hash)
    print(ungeohash(geo_hash[0]))

# you can write to stdout for debugging purposes, e.g.
# print("this is a debug message")
import time,datetime
def solution(S):
    # write your code in Python 3.6
    photos = S.splitlines()
    citymap = {}
    photos_list= []
    for photo in photos:
        photo_data = photo.split(',')
        photo_name_with_ext = photo_data[0].strip()
        photo_name = photo_name_with_ext.split('.')[0]
        photo_ext = photo_name_with_ext.split('.')[1]
        city = photo_data[1].strip()
        time_ = time.mktime(datetime.datetime.strptime(photo_data[2].strip(), '%Y-%m-%d %H:%M:%S').timetuple())
        tmp_photo = Photo(city, photo_name, photo_ext, time_)
        photos_list.append(tmp_photo)
        if city in citymap:
            citymap[city].append(tmp_photo)
        else:
            citymap[city] = [tmp_photo]
    res = []
    # first: find locations 
    sorted_citymap = {}
    for city, photos in citymap.items():
        sorted_photos = sorted(photos, key = lambda photo:photo.time_)
        sorted_citymap[city] = sorted_photos
    for photo in photos_list:
        city = photo.city
        ext = photo.ext
        photo_index = sorted_citymap[city].index(photo)+1
        maxlen = len(sorted_citymap[city])
        padding_len = len(str(maxlen))
        padded = padding(photo_index, padding_len)
        res.append(''.join([city,padded,'.',ext]))
    return '\n'.join(res)
    
def padding(num, maxpad):
        return str(num).zfill(maxpad)
    
    
class Photo:
    def __init__(self, city, name, ext,time_):
        self.name = name
        self.city = city
        self.ext = ext
        self.time_ = time_
    
    def __str__(self):
        return self.name+':'+self.city+':'+self.ext+':'+str(self.time_)
    
    

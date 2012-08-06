Scripts
=======
* Scripts require Python 2.6+

map_ids_backward and map_ids_forward
------------------------------------
If you set the renumber = true option in common.conf, a file step2xr2.txt
is produced in the cache directory. This contains the id > index mapping.
Use map_ids_backward to recover the original ids given a list of indices,
and use map_ids_forward to generate the correct indices given a list of ids.
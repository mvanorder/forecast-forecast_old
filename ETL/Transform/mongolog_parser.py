# parse the mongodb log of object ids from weather data sorting

# str 'sorted_cast_log.txt
# sorted_obs_log.txt = str

file_casts = open('sorted_forecasts.txt', 'r')
casts_log = open('sorted_cast_log.txt', 'a')
while 24:
	# read by character
	char = file_casts.read(24)
	if not char:
		break
	casts_log.write(char + '\n')
file_casts.close()
casts_log.close()

file_obs = open('sorted_observations.txt', 'r')
obs_log = open('sorted_obs_log.txt', 'a')
while 24:
	# read by character
	char = file_obs.read(24)
	if not char:
		break
	obs_log.write(char + '\n')
file_obs.close()
obs_log.close()

# hms-800w-2t_to_influxdb
Read data from Hoymiles HMS-800w-2t and send data to influx database

This small script is using the [hoymiles_wifi python library](https://github.com/suaveolent/hoymiles-wifi) to retrieve the data from and send it to an Influx database.
Some portions of the code are taken from the [ahoy rpi tool](https://github.com/lumapu/ahoy/tree/main/tools/rpi) which is doing basically the same but for the old hoymiles micro inverters.

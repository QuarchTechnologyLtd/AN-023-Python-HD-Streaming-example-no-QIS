#!/usr/bin/env python
import binascii
import _struct
from datetime import datetime
from timeit import default_timer as timer
import socket
import logging
import os, time
import subprocess
from quarchpy.device import *


class HdStreamer:
    def __init__ (self, quarch_device):
        self.__my_device = quarch_device
        self.__socket_recv_size = 512
        self.__header_valid = False
        self.__stream_average_rate = None
        self.__stream_header_version = None
        self.__stream_header_channels = None
        self.__header_size = 4
        self.__sync_packet_active = False
        self.__stream_end_status = -1 # -1 = not status set
        self.__old_socket_timeout = None
        self.__request_stop = False
        self.__mega_buffer = None
        self.__data_store_pos = 0
        self.__stream_decode_state = None
        self.__stream_decode_prev_state = None
        self.__stream_stop_ordered = False
        self.calculate_power = False
        # Members used to buffer and then write to file during the CSV creating process
        self.__write_buffer = ""
        self.__file_stream = None
        # Decode flags to persist across buffers to track cases where words are split
        self.__flag_word_low = False
        self.__val_word_high = 0
        # Counter for the number of stripes processed
        self.stream_stripe_count = 0
        # Socket is buried in a rather ugly way...
        self.__stream_socket = self.__my_device.connectionObj.connection.Connection
        # Data storage for processed output data
        self.processed_data = None

        # State machine setup for the streaming decode.  Provides the state transitions needed for any given channel selection
        # Channel selection is commented in the right. -1 means disabled channel.
        self.__stream_decode = [    (-1,-1,-1,-1),                # 0 0 0 0
                                    (-1,-1,-1, 3),                # 0 0 0 1
                                    (-1,-1, 2,-1),                # 0 0 1 0
                                    (-1,-1, 3, 2),                # 0 0 1 1
                                    (-1, 1,-1,-1),                # 0 1 0 0
                                    (-1, 3,-1, 1),                # 0 1 0 1
                                    (-1, 2, 1,-1),                # 0 1 1 0
                                    (-1, 2, 3, 1),                # 0 1 1 1
                                    ( 0,-1,-1,-1),                # 1 0 0 0
                                    ( 3,-1,-1, 0),                # 1 0 0 1
                                    ( 2,-1, 0,-1),                # 1 0 1 0
                                    ( 2,-1, 3, 0),                # 1 0 1 1
                                    ( 1, 0,-1,-1),                # 1 1 0 0
                                    ( 1, 3,-1, 0),                # 1 1 0 1
                                    ( 1, 2, 0,-1),                # 1 1 1 0
                                    ( 1, 2, 3, 0)]                # 1 1 1 1

        # Check this is a QTL1944 (HD) module code hardware
        if ("1944" not in self.__my_device.sendCommand ("*serial?")):
            raise ValueError ("Attached device not supported.  This code only supports HD power modules (QTL1999 / QTL1995)")

    # Gets the next state for the decode state machine
    def __get_next_decode_state(self, current_state):
        return self.__stream_decode[self.__stream_header_channels][current_state]

    def start_stream (self, seconds, csv_file_path, fio_command=None):
        
        # Work out how many bytes will be in a stripe, based on channel enables
        bytes_per_stripe = 0
        if ("ENABLED" in self.__my_device.sendCommand ("sig:5v:volt:enable?")):
            bytes_per_stripe += 2
        if ("ENABLED" in self.__my_device.sendCommand ("sig:12v:volt:enable?")):
            bytes_per_stripe += 2
        if ("ENABLED" in self.__my_device.sendCommand ("sig:5v:current:enable?")):
            bytes_per_stripe += 4
        if ("ENABLED" in self.__my_device.sendCommand ("sig:12v:current:enable?")):
            bytes_per_stripe += 4
        
        # Get the averaging rate
        ave_rate_str = self.__my_device.sendCommand ("rec:ave?")
        ave_rate = ave_rate_str.split(':')[0].strip()
        if (ave_rate == "0"):
            stripes_per_second = 250000
        elif (ave_rate == "2"):
            stripes_per_second = 125000
        elif (ave_rate == "4"):
            stripes_per_second = 62500
        elif (ave_rate == "8"):
            stripes_per_second = 31250
        elif (ave_rate == "16"):
            stripes_per_second = 15625
        elif (ave_rate == "32"):
            stripes_per_second = 7812.5
        elif (ave_rate == "64"):
            stripes_per_second = 3906.25
        elif (ave_rate == "128"):
            stripes_per_second = 1953.125
        elif (ave_rate == "256"):
            stripes_per_second = 976.5625
        elif (ave_rate == "1k"):
            stripes_per_second = 488.28125
        elif (ave_rate == "2k"):
            stripes_per_second = 244.14062
        elif (ave_rate == "4k"):
            stripes_per_second = 122.07031
        elif (ave_rate == "8k"):
            stripes_per_second = 61.03515
        elif (ave_rate == "16k"):
            stripes_per_second = 30.51758
        elif (ave_rate == "32k"):
            stripes_per_second = 15.25879
        else:
            raise ValueError ("Unknown averaging rate response: " + ave_rate_str)
    
        # Allocate the full buffer now, with 5% additional
        mega_buffer_len = int(seconds * stripes_per_second * bytes_per_stripe * 1.05)

        # Create initial receive buffers, avoiding repeat allocation                
        self.__mega_buffer = bytearray(mega_buffer_len)
        self.__data_store_pos = 0
        data_buffer_len = 1024
        data = bytearray(data_buffer_len)
        len_data = bytearray(2)

        # Tell the PPM we are stream capable, to unlock the stream function
        self.__my_device.sendCommand ("conf stream enable on")
        # Start stream
        self.__my_device.sendCommand ("rec stream")

        if (fio_command is not None):
            print("Starting FIO workload")
            fio_formatted_cmd = fio_command.split(" ")
            myproc = subprocess.Popen(fio_formatted_cmd)

        # Loop to get all data in the stream, not returning until done
        stream_start = timer()
        while self.__stream_end_status == -1:            
            # Read packet size first
            self.__stream_socket.recv_into (memoryview(len_data), 2)
                           
            if not len_data:
                break
            len_bytes = int(len_data[0] + (len_data[1] << 8))

            # If the current buffer is not large enough, re-allocate with a bit of headroom
            if (len_bytes > data_buffer_len):
                data = bytearray(len_bytes + 10)

            # Receive from the socket into the end of the current data
            self.__stream_socket.recv_into(memoryview(data), len_bytes)
            # Force an TCP ACK by sending a stub packet, used to speed up the data flow
            self.__stream_socket.send(bytearray(b'\x02\x00\xff\xff'))

            # If header is not valid, assume this is the stream header and process it (one time operation at start)
            if (self.__header_valid == False):          
                self.__process_stream_header (data)
                self.__header_valid = True
                len_bytes -= self.__header_size

                # If we're done after the header bytes, continue and skip processing
                if (len_bytes == 0):
                    continue
            
            # Odd byte count means a status byte at the end which must be processed
            if ((len_bytes & 1) != 0):
                self.__handle_status_byte (data[len_bytes-1])
                len_bytes -= 1

            if (len_bytes > 0):
                # Store data into the stream mega buffer, at the end of current data (no processing during the stream, to avoid any performance hit)
                self.__mega_buffer[self.__data_store_pos:self.__data_store_pos + len_bytes] = data[0:len_bytes]
                self.__data_store_pos += len_bytes

            # Perform ACK sequence as required by the current status
            if (self.__sync_packet_active):
                self.__send_sync()
                self.__sync_packet_active = False                        
            
            # End after set time
            if (timer() - stream_start > seconds):
                self.__request_stop = True                      

        if (fio_command is not None):
            # If this while loop is entered, the stream time is too short for the FIO job.
            while myproc.poll() is None:
                print("Waiting for FIO to finish..")
                time.sleep(1)

        print ("Processing data to CSV")
        # Stream has now fully completed, write the data to csv, as required
        self.__file_stream = open (csv_file_path, 'w')
        # write capture data        
        #self.__file_stream.write ("Record Time: " + datetime.now().strftime("%m/%d/%Y %H:%M:%S"))
        #self.__file_stream.write (datetime.now().strftime(", Recording Rate:" + ave_rate_str + "\r"))
        self.__file_stream.write ("Time us,")
        # Write header (based on active channels)
        if (self.__stream_header_channels & 0x0008) != 0:                       # 5V V
            self.__file_stream.write ("5V voltage mV,")
        if (self.__stream_header_channels & 0x0004) != 0:                       # 5V I
            self.__file_stream.write ("5V current uA,")
        if (self.__stream_header_channels & 0x0002) != 0:                       # 12V V
            self.__file_stream.write ("12V voltage mV,")
        if (self.__stream_header_channels & 0x0001) != 0:                       # 12V I
            self.__file_stream.write ("12V current uA,")
        if (self.__stream_header_channels & 0x0008 != 0 and self.__stream_header_channels & 0x0004 != 0):
            self.__file_stream.write ("5V power uW,")
        if (self.__stream_header_channels & 0x0002 != 0 and self.__stream_header_channels & 0x0001 != 0):
            self.__file_stream.write ("12V power uW,")
        if (self.__stream_header_channels & 0x0008 != 0 and self.__stream_header_channels & 0x0004 != 0 and self.__stream_header_channels & 0x0002 != 0 and self.__stream_header_channels & 0x0001 != 0):
            self.__file_stream.write ("Total power uW")
        self.__file_stream.write ("\r")

        # Process the buffered data
        self.__decode_stream_data_buffer (memoryview(self.__mega_buffer))
        self.__file_stream.close()
        print ("CSV created, process complete!")
            

    # Processes each packet comming in
    def __process_packet (self, data):
        # If header is not valid, assume this is the stream header and process it
        if (self.__header_valid == False):          
            self.__process_stream_header (data)
            self.__header_valid = True

            # process remainder as regular stream data
            if (len(data) > self.__header_size):
                data = data[self.__header_size:]
                self.__process_stream_data (data)            
        # Else process as a regular stream data packet
        else:
            self.__process_stream_data (data)

    # Handles the status byte and sets actions based on it
    def __handle_status_byte(self, status_byte):
        # Termination request on 0-2
        if (status_byte >= 0 and status_byte < 3):
            self.__stream_end_status = status_byte            
        # No data yet on 3
        elif (status_byte == 3):
            pass
        # Sync packet, must be replied to on 7
        elif (status_byte == 7):
            self.__sync_packet_active = True
        # Else bad status byte
        else:
            print ("BAD STATUS: " + str(status_byte))

    # Processing for stream data
    def __process_stream_data (self, data):       
        data_len = len(data)
  
        # Odd byte count means a status byte at the end
        if ((data_len & 1) != 0):
            self.__handle_status_byte (data[data_len-1])
            data_len -= 1

        if (data_len > 0):
            # Store data into the stream mega buffer, at the end of current data (no processing during the stream, to avoid any performance hit)
            #mem_view = memoryview(self.__mega_buffer)[self.__data_store_pos:]
            self.__mega_buffer[self.__data_store_pos:self.__data_store_pos + data_len] = data[0:data_len]
            #mem_view[:] = bytes(b'hello')#data[0:data_len]
            self.__data_store_pos += data_len

        # Perform ACK sequence as required by the current status
        if (self.__sync_packet_active):
            self.__send_sync()
            self.__sync_packet_active = False

    # Send an ACK packet to the SYNC request, allowing streaming to continue
    def __send_sync(self):
        if (self.__request_stop == True):
            self.__my_device.sendCommand ("rec stop")         
            if (self.__stream_stop_ordered == False):
                self.__stream_stop_ordered = True
                print ("Stopping stream, recording time is complete")
        self.__stream_socket.send (bytearray(b'\x02\x00\xff\x01'))        
        # print ("ACK")

    # Processing for stream header
    def __process_stream_header (self, data):
        self.__stream_header_version = data[0]
        # Future code may need header size to change based on version here
        # data[1] is a reserved padding byte
        self.__stream_header_channels = data[2]
        self.__stream_average_rate = data[3]
        # Calculate the initial stream state given the available channels
        self.__init_stream_state (self.__stream_header_channels & 0x0F)

    # Set the initial decode state, given the channels that will be returned.  This comes from the stream header
    def __init_stream_state(self, channel_enable):
        if (channel_enable & 0x0008) != 0:                         # 5V V
            self.__stream_decode_state = 0

        elif (channel_enable & 0x0004) != 0:                       # 5V I
            self.__stream_decode_state = 1

        elif (channel_enable & 0x0002) != 0:                       # 12V V
            self.__stream_decode_state = 2

        elif (channel_enable & 0x0001) != 0:                       # 12V I
            self.__stream_decode_state = 3

        if self.__stream_decode_state == -1:
            raise ValueError('Device header indicates that no channels are enabled for streaming')

    # Makes sure this looks like a header (basic check only)
    def __is_header_valid (self, data):
        ret_val = False

        if (len(data) > 1):
            if (data[0] == 5):
                ret_val = True

        return ret_val

    # Converts a buffer element to word
    def __stream_buffer_to_word(self, buffer):
        wordVal = (_struct.unpack( '<h', buffer)[0]) 
        wordVal = wordVal & 0x3fff
        
        return wordVal



    # Decodes a buffer of streaming data measurements.  The header and and additional transport bytes must have been removed by this point, leaving only
    # pure measurement data.  The decode uses a state machine, initialised by the header bytes at the start of streaming.  Multiple calls can be made
    # provided sequential buffers of valid data are given.
    def __decode_stream_data_buffer(self,  buffer):

        i = 0
        buffer_len = len(buffer)
        str_cols = ["","","","","","","",""]
        value_cols = [0,0,0,0,0,0,0,0]        


        # Note: Fixed HD 4uS per stripe base measurement rate
        ave_multiplier = (pow(self.__stream_average_rate,2) * 4)        

        while (i < buffer_len):
            toBuffer = False
            if (self.__stream_decode_state == 0):
                # scale 5V V and write to file
                intVal = self.__stream_buffer_to_word( buffer[i:i+2] )        
                value_cols[1] = intVal
                toBuffer = True                
                i = i + 2

            if (self.__stream_decode_state == 1):
                # scale 5V I and write to file
                if self.__flag_word_low:
                    self.__flag_word_low = False
                    intVal = self.__stream_buffer_to_word( buffer[i:i+2] )
                    value_cols[2] = ( (self.__val_word_high * 4096) + (intVal & 0x3fff) )
                    toBuffer = True                    
                else:
                    self.__flag_word_low = True
                    self.__val_word_high = self.__stream_buffer_to_word( buffer[i:i+2] )
                i = i + 2

            if (self.__stream_decode_state == 2):
                # scale 12V V and write to file
                intVal = self.__stream_buffer_to_word( buffer[i:i+2] )
                value_cols[3] = intVal
                toBuffer = True
                i = i + 2

            if (self.__stream_decode_state == 3):
                # scale 12V I and write to file
                if self.__flag_word_low:
                    self.__flag_word_low = False
                    intVal = self.__stream_buffer_to_word( buffer[i:i+2] )
                    value_cols[4] = ( (self.__val_word_high * 4096) + (intVal & 0x3fff) )
                    toBuffer = True
                else:
                    self.__flag_word_low = True
                    self.__val_word_high = self.__stream_buffer_to_word( buffer[i:i+2] )

                i = i + 2

            if self.__flag_word_low == False:
                self.__stream_decode_prev_state = self.__stream_decode_state
                self.__stream_decode_state = self.__get_next_decode_state(self.__stream_decode_state)

            # If meas ready to be output
            if toBuffer:                
                #self.__write_buffer = self.__write_buffer + '{:.4f}'.format(fpVal)
                if( self.__stream_decode_state <= self.__stream_decode_prev_state ):
                    # Calculate time and power values
                    value_cols[0] = (self.stream_stripe_count * ave_multiplier)
                    value_cols[5] = (value_cols[1] * value_cols[2])/1000
                    value_cols[6] = (value_cols[3] * value_cols[4])/1000
                    value_cols[7] = value_cols[5] + value_cols[6]
                    # Format to strings and write to file in CSV form                    
                    self.__file_stream.write(','.join (map(str, value_cols)) + "\r")                                                                           
                    # Next stripe
                    self.stream_stripe_count = self.stream_stripe_count + 1


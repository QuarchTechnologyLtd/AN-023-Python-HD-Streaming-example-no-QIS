#!/usr/bin/env python
import binascii
import _struct
from datetime import datetime
from timeit import default_timer as timer
import socket
import logging
import os, time
import subprocess
from io import StringIO
from quarchpy.device import *
import threading


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
        self.__next_data_pos = 0
        self.__stream_decode_state = None
        self.__stream_decode_prev_state = None
        self.__stream_stop_ordered = False
        self.calculate_power = False
        self.__real_time_thread_running = False
        self.dump_complete = False
        self.__save_mode = "post_process"
        self.__csv_file_path = None
        # Members used to buffer and then write to file during the CSV creating process
        self.__write_buffer = ""
        self.__file_stream = None
        # Decode flags to persist across buffers to track cases where words are split
        self.__flag_word_low = False
        self.__val_word_high = 0
        # Tracks the time of the next stripe to be written
        self.stream_time_pos = 0
        # Socket is buried in a rather ugly way...
        self.__stream_socket = self.__my_device.connectionObj.connection.Connection
        # Data storage for processed output data
        self.processed_data = None   
        # Temp holding for data processing the final stripes
        self.__value_time = 0
        self.__value_12v = 0
        self.__value_12i = 0
        self.__value_5v = 0
        self.__value_5i = 0
        self.__value_12p = 0
        self.__value_5p = 0
        self.__value_totp = 0

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

    def start_stream (self, seconds, csv_file_path, fio_command=None, save_mode="post_process"):

        self.__save_mode = save_mode
        self.__csv_file_path = csv_file_path
        
        # Work out how many bytes will be in a stripe, based on channel enables
        bytes_per_stripe = 0
        if ("ON" in self.__my_device.sendCommand ("rec:5v:volt:enable?")):
            bytes_per_stripe += 2
        if ("ON" in self.__my_device.sendCommand ("rec:12v:volt:enable?")):
            bytes_per_stripe += 2
        if ("ON" in self.__my_device.sendCommand ("rec:5v:current:enable?")):
            bytes_per_stripe += 4
        if ("ON" in self.__my_device.sendCommand ("rec:12v:current:enable?")):
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
        self.__mega_buffer = bytearray(0)#mega_buffer_len)
        logging.debug(datetime.now().isoformat() + "\t: Init megabuffer as length: " + str(mega_buffer_len))
        self.__data_store_pos = 0
        data_buffer_len = 1024
        data = bytearray(data_buffer_len)
        len_data = bytearray(2)

        # Tell the PPM we are stream capable, to unlock the stream function
        self.__my_device.sendCommand ("conf stream enable on")
        # Start stream
        self.__my_device.sendCommand ("rec stream")
        
        logging.debug(datetime.now().isoformat() + "\t: Stream Started")

        if (fio_command is not None):
            print("Starting FIO workload")
            fio_formatted_cmd = fio_command.split(" ")
            myproc = subprocess.Popen(fio_formatted_cmd)            
            logging.debug(datetime.now().isoformat() + "\t: FIO workload Started")    
            
        # Start threaded real-time save process if requested
        if (save_mode == "real_time"):
            proc_thread = threading.Thread(name='save_worker', target=self.__decode_stream_section_worker)
            proc_thread.start()

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
                if (self.__request_stop == False):
                    logging.debug(datetime.now().isoformat() + "\t: Stream time complete, halting")
                    self.__request_stop = True        
                

        if (fio_command is not None):
            # If this while loop is entered, the stream time is too short for the FIO job.
            while myproc.poll() is None:
                print("Waiting for FIO to finish..")
                logging.debug(datetime.now().isoformat() + "\t: Waiting for FIO to finish...")
                time.sleep(1)

        if (save_mode == "post_process"):
            
            print ("Post-processing data to CSV")
            logging.debug(datetime.now().isoformat() + "\t: Started CSV post-processing")
            
            # Stream has now fully completed, write the data to csv, as required
            self.__prepare_csv_file (csv_file_path)
            
            # Stream has now fully completed, write the data to csv, as required
            #self.__file_stream = open (csv_file_path, 'w')
            # write capture data        
            #self.__file_stream.write ("Record Time: " + datetime.now().strftime("%m/%d/%Y %H:%M:%S"))
            #self.__file_stream.write (datetime.now().strftime(", Recording Rate:" + ave_rate_str + "\r"))
            #self.__file_stream.write ("Time us,")
            # Write header (based on active channels)
            #if (self.__stream_header_channels & 0x0008) != 0:                       # 5V V
             #   self.__file_stream.write ("5V voltage mV,")
            #if (self.__stream_header_channels & 0x0004) != 0:                       # 5V I
            #    self.__file_stream.write ("5V current uA,")
            #if (self.__stream_header_channels & 0x0002) != 0:                       # 12V V
            #    self.__file_stream.write ("12V voltage mV,")
            #if (self.__stream_header_channels & 0x0001) != 0:                       # 12V I
            #    self.__file_stream.write ("12V current uA,")
            #if (self.__stream_header_channels & 0x0008 != 0 and self.__stream_header_channels & 0x0004 != 0):
            #    self.__file_stream.write ("5V power uW,")
            #if (self.__stream_header_channels & 0x0002 != 0 and self.__stream_header_channels & 0x0001 != 0):
            #    self.__file_stream.write ("12V power uW,")
            #if (self.__stream_header_channels & 0x0008 != 0 and self.__stream_header_channels & 0x0004 != 0 and self.__stream_header_channels & 0x0002 != 0 and self.__stream_header_channels & 0x0001 != 0):
            #    self.__file_stream.write ("Total power uW")
            #self.__file_stream.write ("\r")

            # Process the buffered data
            self.__decode_stream_data_buffer (memoryview(self.__mega_buffer))
            self.__file_stream.close()
            logging.debug(datetime.now().isoformat() + "\t: Completed CSV post-processing, exiting")
            print ("CSV created, process complete!")
        elif (save_mode == "real_time"):
            # Wait here until the thread based save is complete
            while (self.__real_time_thread_running):
                self.dump_complete = True
                logging.debug(datetime.now().isoformat() + "\t: Waiting for save to complete")
                time.sleep(1)
        else:
            raise ValueError ("Invalid save mode: " + save_mode)
            

    def __prepare_csv_file (self, path):
        self.__file_stream = open (path, 'w')
        
        # Write header (based on active channels)
        self.__file_stream.write ("Time us,")        
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
            if (status_byte == 1):
                print ("BUFFER OVERRUN - Stream stopped early")
            logging.debug(datetime.now().isoformat() + "\t: Stream download from PPM complete: " + str(status_byte))
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

    # Processing for stream header
    def __process_stream_header (self, data):
        self.__stream_header_version = data[0]
        # Future code may need header size to change based on version here
        # data[1] is a reserved padding byte
        self.__stream_header_channels = data[2]
        self.__stream_average_rate = data[3]
        # Calculate the initial stream state given the available channels
        self.__init_stream_state (self.__stream_header_channels & 0x0F)

        # Prepate the output file for real time write if required
        if (self.__file_stream is None and self.__save_mode == "real_time"):
            logging.debug(datetime.now().isoformat() + "\t: Opened CSV for real-time processing")                        
            self.__prepare_csv_file (self.__csv_file_path)

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
        wordVal = int.from_bytes(buffer, byteorder='little', signed=False)
        #wordVal = (_struct.unpack( '<h', buffer)[0]) 
        wordVal = wordVal & 0x3fff
        
        return wordVal

    # Thread worker function, used to process stream data while recording
    def __decode_stream_section_worker (self):
        self.__real_time_thread_running = True

        buffer_needed = 1500
        process_size = 1000

        logging.debug(datetime.now().isoformat() + "\t: Real-time save worker: Started")
        
        # While there is data to process
        while (self.__data_store_pos >= self.__next_data_pos and self.dump_complete == False):

            # If there are more than x bytes to process, process a block
            next_data = self.__next_data_pos
            store_pos = self.__data_store_pos            
            if (store_pos - next_data > buffer_needed):
                #logging.debug(datetime.now().isoformat() + "\t: Real-time save worker: Decode " + str(next_data) + "-" + str(next_data+process_size))
                self.__decode_stream_data_buffer (self.__mega_buffer[next_data:next_data+process_size])
                self.__next_data_pos = (next_data + process_size)
            # Else wait a little for more to come along
            else:
                time.sleep (0.25)
                    
        logging.debug(datetime.now().isoformat() + "\t: Real-time save worker: Final processing")

        # Process all remaining bytes
        next_data = self.__next_data_pos
        store_pos = self.__data_store_pos       
        #logging.debug(datetime.now().isoformat() + "\t: Real-time save worker: Decode " + str(next_data) + "-" + str(self.__data_store_pos))
        self.__decode_stream_data_buffer (self.__mega_buffer[next_data:])    

        logging.debug(datetime.now().isoformat() + "\t: Real-time save worker: Closing")
        
        self.__real_time_thread_running = False
        

    # Decodes a buffer of streaming data measurements.  The header and and additional transport bytes must have been removed by this point, leaving only
    # pure measurement data.  The decode uses a state machine, initialised by the header bytes at the start of streaming.  Multiple calls can be made
    # provided sequential buffers of valid data are given.
    def __decode_stream_data_buffer(self,  buffer):

        i = 0
        buffer_len = len(buffer)
        #str_cols = ["","","","","","","",""]
        #value_cols = [0,0,0,0,0,0,0,0]
        value_time = 0
        #self.__value_12v = 0
        #self.__value_12i = 0
        #self.__value_5v = 0
        #self.__value_5i = 0
        value_12p = 0
        value_5p = 0
        value_totp = 0
        # intVal = 0
        # Additional buffer used to avoid so many file IO writes
        #output_buffer = StringIO()
        #buffer_limit = 1000
        #buffer_size = 0


        # Note: Fixed HD 4uS per stripe base measurement rate
        ave_multiplier = (pow(self.__stream_average_rate,2) * 4)   
        if (ave_multiplier == 0):
            ave_multiplier = 4

        while (i < buffer_len):
            toBuffer = False
            if (self.__stream_decode_state == 0):
                # scale 5V V and write to file
                #intVal = self.__stream_buffer_to_word( buffer[i:i+2] )        
                self.__value_5v = (int.from_bytes(buffer[i:i+2], byteorder='little', signed=False) & 0x3FFF)
                #value_5v = intVal
                toBuffer = True                
                i = i + 2

            elif (self.__stream_decode_state == 1):
                # scale 5V I and write to file
                if self.__flag_word_low:
                    self.__flag_word_low = False
                    #intVal = self.__stream_buffer_to_word( buffer[i:i+2] )
                    #value_5i = ( (self.__val_word_high * 4096) + (intVal & 0x3fff) )
                    self.__value_5i = (self.__val_word_high * 4096)
                    #value_5i = (self.__val_word_high << 8)
                    self.__value_5i = self.__value_5i + (int.from_bytes(buffer[i:i+2], byteorder='little', signed=False) & 0x3FFF)
                    toBuffer = True                    
                else:
                    self.__flag_word_low = True
                    #self.__val_word_high = self.__stream_buffer_to_word( buffer[i:i+2] )
                    self.__val_word_high = (int.from_bytes(buffer[i:i+2], byteorder='little', signed=False) & 0x3FFF)
                i = i + 2

            elif (self.__stream_decode_state == 2):
                # scale 12V V and write to file
                #intVal = self.__stream_buffer_to_word( buffer[i:i+2] )
                #value_12v = intVal
                self.__value_12v = (int.from_bytes(buffer[i:i+2], byteorder='little', signed=False) & 0x3FFF)
                toBuffer = True
                i = i + 2

            elif (self.__stream_decode_state == 3):
                # scale 12V I and write to file
                if self.__flag_word_low:
                    self.__flag_word_low = False
                    #intVal = self.__stream_buffer_to_word( buffer[i:i+2] )
                    #value_12i = ( (self.__val_word_high * 4096) + (intVal & 0x3fff) )
                    self.__value_12i = (self.__val_word_high * 4096)
                    #value_12i = (self.__val_word_high << 8)
                    self.__value_12i = self.__value_12i + (int.from_bytes(buffer[i:i+2], byteorder='little', signed=False) & 0x3FFF)
                    toBuffer = True
                else:
                    self.__flag_word_low = True
                    #self.__val_word_high = self.__stream_buffer_to_word( buffer[i:i+2] )
                    self.__val_word_high = (int.from_bytes(buffer[i:i+2], byteorder='little', signed=False) & 0x3FFF)

                i = i + 2

            if self.__flag_word_low == False:
                self.__stream_decode_prev_state = self.__stream_decode_state
                self.__stream_decode_state = self.__get_next_decode_state(self.__stream_decode_state)

            # If meas ready to be output
            if toBuffer:                               
                if( self.__stream_decode_state <= self.__stream_decode_prev_state ):
                    # Calculate time and power values                    
#                    calc_a = timer()
                    value_time = self.stream_time_pos
                    value_5p = (self.__value_5v * self.__value_5i)/1000
                    value_12p = (self.__value_12v * self.__value_12i)/1000
                    value_totp = value_5p + value_12p
#                    calc_b = timer()
 #                   if (i % 1000 == 0):
  #                      logging.debug(str(calc_b - calc_a) + "\t: calc")
                    # Format to strings and write to file in CSV form.  This tested as the best performance concat
 #                   calc_a = timer()    
                    #self.__file_stream.write(','.join (map(str, value_cols)) + "\r") 
                    self.__file_stream.write(str(value_time) + "," + str(self.__value_5v) + "," + str(self.__value_5i) + "," + str(self.__value_12v) + "," + str(self.__value_12i) + "," + str(value_5p) + "," + str(value_12p) + "," + str(value_totp) + "\r")
                    #output_buffer.write (str(value_cols[0]) + "," + str(value_cols[1]) + "," + str(value_cols[2]) + "," + str(value_cols[3]) + "," + str(value_cols[4]) + "," + str(value_cols[5]) + "," + str(value_cols[6]) + "," + str(value_cols[7]) + "\r")
                    #buffer_size = buffer_size + 1
                    #if (buffer_size > buffer_limit):
                     #   self.__file_stream.write (output_buffer.getvalue())
                      #  buffer_size = 0
                       # output_buffer.close()
                        #output_buffer = StringIO()
                    #self.__file_stream.write(str(value_cols[0]))  
                    #self.__file_stream.write(",")  
                    #self.__file_stream.write(str(value_cols[1]))  
                    #self.__file_stream.write(",")  
                    #self.__file_stream.write(str(value_cols[2]))  
                    #self.__file_stream.write(",")  
                    #self.__file_stream.write(str(value_cols[3]))  
                    #self.__file_stream.write(",")  
                    #self.__file_stream.write(str(value_cols[4]))  
                    #self.__file_stream.write(",")  
                    #self.__file_stream.write(str(value_cols[5]))  
                    #self.__file_stream.write(",")  
                    #self.__file_stream.write(str(value_cols[6]))  
                    #self.__file_stream.write(",")  
                    #self.__file_stream.write(str(value_cols[7]))  
                    #self.__file_stream.write("\r")  
 #                   calc_b = timer()
  #                  if (i % 1000 == 0):
   #                     logging.debug(str(calc_b - calc_a) + "\t: write")
                    # Next stripe
                    self.stream_time_pos = self.stream_time_pos + ave_multiplier


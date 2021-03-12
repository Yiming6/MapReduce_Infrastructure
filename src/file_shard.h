#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include <math.h>

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard
{
     int id;
     std::vector<std::string> files;
     std::vector<int> starts;
     std::vector<int> ends;
};

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec &mr_spec, std::vector<FileShard> &fileShards)
{
     int total = 0;
     for (std::string file : mr_spec.input_files)
     {
          std::ifstream input(file, std::ios::ate | std::ios::binary);
          input.seekg(0, input.end);
          total += input.tellg();
          input.seekg(0, input.beg);
     }
     int n_shards = ceil(total / (mr_spec.map_kilobytes * 1024));
     int id = 0;
     int curr_file = 0;
     int curr_idx = 0;
     for (int i = 0; i < n_shards - 1; ++i)
     {
          FileShard fileShard;
          fileShard.id = id;
          id += 1;
          fileShard.files.push_back(mr_spec.input_files[curr_file]);
          fileShard.starts.push_back(curr_idx);
          int curr_size = 0;
          while (curr_size < mr_spec.map_kilobytes * 1024)
          {
               std::ifstream input(mr_spec.input_files[curr_file], std::ios::ate | std::ios::binary);
               input.seekg(0, input.end);
               int file_size = input.tellg();
               input.seekg(0, input.beg);
               if (curr_size + file_size - curr_idx < mr_spec.map_kilobytes * 1024)
               {
                    fileShard.ends.push_back(file_size);
                    curr_size += file_size - curr_idx;
                    curr_file += 1;
                    curr_idx = 0;
                    fileShard.files.push_back(mr_spec.input_files[curr_file]);
                    fileShard.starts.push_back(curr_idx);
               }
               else if (curr_size + file_size - curr_idx == mr_spec.map_kilobytes * 1024)
               {
                    fileShard.ends.push_back(file_size);
                    curr_size += file_size - curr_idx;
                    curr_file += 1;
                    curr_idx = 0;
               }
               else
               {
                    input.seekg(curr_idx);
                    while (curr_size < mr_spec.map_kilobytes * 1024)
                    {
                         std::string line;
                         std::getline(input, line);
                         int new_idx = input.tellg();
                         curr_size += new_idx - curr_idx;
                         curr_idx = new_idx;
                    }
                    fileShard.ends.push_back(curr_idx);
                    if (curr_idx == file_size)
                    {
                         curr_file += 1;
                         curr_idx = 0;
                    }
               }
          }
          fileShards.push_back(fileShard);
     }
     FileShard fileShard;
     fileShard.id = id;
     while (curr_file < mr_spec.input_files.size())
     {
          fileShard.files.push_back(mr_spec.input_files[curr_file]);
          fileShard.starts.push_back(curr_idx);
          std::ifstream input(mr_spec.input_files[curr_file], std::ios::ate | std::ios::binary);
          input.seekg(0, input.end);
          int file_size = input.tellg();
          input.seekg(0, input.beg);
          fileShard.ends.push_back(file_size);
          curr_file += 1;
          curr_idx = 0;
     }
     fileShards.push_back(fileShard);
     return true;
}
#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec
{
	int n_workers;
	std::vector<std::string> worker_ipaddr_ports;
	std::vector<std::string> input_files;
	std::string output_dir;
	int n_output_files;
	int map_kilobytes;
	std::string user_id;
};

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string &config_filename, MapReduceSpec &mr_spec)
{
	std::ifstream config(config_filename);
	if (config.good() == false)
	{
		std::cout << "Config file miss" << std::endl;
		return false;
	}
	std::string line;
	while (std::getline(config, line))
	{
		std::stringstream specs(line);
		std::string segment;
		std::vector<std::string> spec;
		while (std::getline(specs, segment, '='))
		{
			spec.push_back(segment);
		}
		if (spec[0].compare("n_workers") == 0)
		{
			mr_spec.n_workers = atoi(spec[1].c_str());
		}
		else if (spec[0].compare("worker_ipaddr_ports") == 0)
		{
			std::stringstream ports(spec[1]);
			std::string port;
			while (std::getline(ports, port, ','))
			{
				mr_spec.worker_ipaddr_ports.push_back(port);
			}
		}
		else if (spec[0].compare("input_files") == 0)
		{
			std::stringstream files(spec[1]);
			std::string file;
			while (std::getline(files, file, ','))
			{
				mr_spec.input_files.push_back(file);
			}
		}
		else if (spec[0].compare("output_dir") == 0)
		{
			mr_spec.output_dir = spec[1];
		}
		else if (spec[0].compare("n_output_files") == 0)
		{
			mr_spec.n_output_files = atoi(spec[1].c_str());
		}
		else if (spec[0].compare("map_kilobytes") == 0)
		{
			mr_spec.map_kilobytes = atoi(spec[1].c_str());
		}
		else if (spec[0].compare("user_id") == 0)
		{
			mr_spec.user_id = spec[1];
		}
	}
	return true;
}

/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec &mr_spec)
{
	if (mr_spec.n_workers != mr_spec.worker_ipaddr_ports.size())
	{
		std::cout << "Worker number wrong" << std::endl;
		return false;
	}
	for (std::string file : mr_spec.input_files)
	{
		std::ifstream input(file);
		if (input.good() == false)
		{
			std::cout << "Input file " << file << " miss" << std::endl;
			return false;
		}
	}
	std::cout << "///////" << std::endl;
	std::cout << "n_workers: " << mr_spec.n_workers << std::endl;
	std::cout << "worker_ipaddr_ports: " << std::endl;
	for (std::string port : mr_spec.worker_ipaddr_ports)
	{
		std::cout << "\t" << port << std::endl;
	}
	std::cout << "input_files: " << std::endl;
	for (std::string file : mr_spec.input_files)
	{
		std::cout << "\t" << file << std::endl;
	}
	std::cout << "output_dir: " << mr_spec.output_dir << std::endl;
	std::cout << "n_output_files: " << mr_spec.n_output_files << std::endl;
	std::cout << "map_kilobytes: " << mr_spec.map_kilobytes << std::endl;
	std::cout << "user_id: " << mr_spec.user_id << std::endl;
	std::cout << "///////" << std::endl;
	return true;
}
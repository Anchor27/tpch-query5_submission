#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <set>
#include <unordered_map>
#include <iomanip>
#include <vector>
#include <map>
#include <string>

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    r_name = "";
    start_date = "";
    end_date = "";
    num_threads = 1;
    table_path = "";
    result_path = "";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--r_name" && i + 1 < argc) r_name = argv[++i];
        else if (arg == "--start_date" && i + 1 < argc) start_date = argv[++i];
        else if (arg == "--end_date" && i + 1 < argc) end_date = argv[++i];
        else if (arg == "--threads" && i + 1 < argc) num_threads = std::stoi(argv[++i]);
        else if (arg == "--table_path" && i + 1 < argc) table_path = argv[++i];
        else if (arg == "--result_path" && i + 1 < argc) result_path = argv[++i];
        else {
            std::cerr << "Unknown or incomplete argument: " << arg << "\n";
            return false;
        }
    }

    if (r_name.empty() || start_date.empty() || end_date.empty() || table_path.empty() || result_path.empty()) {
        std::cerr << "Missing required arguments.\n";
        return false;
    }
    if (num_threads <= 0) {
        std::cerr << "Number of threads must be positive.\n";
        return false;
    }
    return true;
}

// Helper to read a table file into a vector of row maps
bool readTPCHData(const std::string& table_path,
    std::vector<std::map<std::string, std::string>>& customer_data,
    std::vector<std::map<std::string, std::string>>& orders_data,
    std::vector<std::map<std::string, std::string>>& lineitem_data,
    std::vector<std::map<std::string, std::string>>& supplier_data,
    std::vector<std::map<std::string, std::string>>& nation_data,
    std::vector<std::map<std::string, std::string>>& region_data) {

    auto read_tbl = [](const std::string& filepath, const std::vector<std::string>& columns, std::vector<std::map<std::string, std::string>>& output) {
        std::ifstream file(filepath);
        if (!file.is_open()) {
            std::cerr << "Failed to open file: " << filepath << "\n";
            return;
        }

        std::string line;
        while (std::getline(file, line)) {
            std::stringstream ss(line);
            std::string token;
            std::map<std::string, std::string> row;
            int i = 0;

            while (std::getline(ss, token, '|') && i < static_cast<int>(columns.size())) {
                row[columns[i++]] = token;
            }
            output.push_back(std::move(row));
        }
    };

    read_tbl(table_path + "/region.tbl",   {"regionkey", "name", "comment"}, region_data);
    read_tbl(table_path + "/nation.tbl",   {"nationkey", "name", "regionkey", "comment"}, nation_data);
    read_tbl(table_path + "/customer.tbl", {"custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"}, customer_data);
    read_tbl(table_path + "/orders.tbl",   {"orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment"}, orders_data);
    read_tbl(table_path + "/lineitem.tbl", {"orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount", "tax", "returnflag", "linestatus", "shipdate", "commitdate", "receiptdate", "shipinstruct", "shipmode", "comment"}, lineitem_data);
    read_tbl(table_path + "/supplier.tbl", {"suppkey", "name", "address", "nationkey", "phone", "acctbal", "comment"}, supplier_data);

    return true;
}

// Core query execution
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads,
                   const std::vector<std::map<std::string, std::string>>& customer_data,
                   const std::vector<std::map<std::string, std::string>>& orders_data,
                   const std::vector<std::map<std::string, std::string>>& lineitem_data,
                   const std::vector<std::map<std::string, std::string>>& supplier_data,
                   const std::vector<std::map<std::string, std::string>>& nation_data,
                   const std::vector<std::map<std::string, std::string>>& region_data,
                   std::map<std::string, double>& results) {

    std::mutex result_mutex;

    // 1. Region key for the specified region name
    std::string target_regionkey;
    for (const auto& row : region_data) {
        if (row.at("name") == r_name) {
            target_regionkey = row.at("regionkey");
            break;
        }
    }
    if (target_regionkey.empty()) return false;

    // 2. Nation keys in that region
    std::set<std::string> target_nationkeys;
    std::unordered_map<std::string, std::string> nationkey_to_name;
    for (const auto& row : nation_data) {
        nationkey_to_name[row.at("nationkey")] = row.at("name");
        if (row.at("regionkey") == target_regionkey) {
            target_nationkeys.insert(row.at("nationkey"));
        }
    }

    // 3. Supplier and customer filtering
    std::set<std::string> target_suppkeys, target_custkeys;
    std::unordered_map<std::string, std::string> suppkey_to_nationkey;
    for (const auto& row : supplier_data) {
        if (target_nationkeys.count(row.at("nationkey"))) {
            target_suppkeys.insert(row.at("suppkey"));
            suppkey_to_nationkey[row.at("suppkey")] = row.at("nationkey");
        }
    }
    for (const auto& row : customer_data) {
        if (target_nationkeys.count(row.at("nationkey"))) {
            target_custkeys.insert(row.at("custkey"));
        }
    }

    // 4. Orders that match customer and date
    std::set<std::string> target_orderkeys;
    for (const auto& row : orders_data) {
        if (target_custkeys.count(row.at("custkey")) &&
            row.at("orderdate") >= start_date &&
            row.at("orderdate") < end_date) {
            target_orderkeys.insert(row.at("orderkey"));
        }
    }

    // 5. Parallel lineitem processing
    auto worker = [&](int tid, int total_threads) {
        std::map<std::string, double> local_result;
        for (size_t i = tid; i < lineitem_data.size(); i += total_threads) {
            const auto& row = lineitem_data[i];

            const std::string& suppkey = row.at("suppkey");
            const std::string& orderkey = row.at("orderkey");
            if (!target_suppkeys.count(suppkey)) continue;
            if (!target_orderkeys.count(orderkey)) continue;

            double extendedprice = std::stod(row.at("extendedprice"));
            double discount = std::stod(row.at("discount"));
            double revenue = extendedprice * (1.0 - discount);

            const std::string& nationkey = suppkey_to_nationkey[suppkey];
            const std::string& nation_name = nationkey_to_name[nationkey];
            local_result[nation_name] += revenue;
        }

        std::lock_guard<std::mutex> lock(result_mutex);
        for (const auto& pair : local_result) {
            results[pair.first] += pair.second;
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i, num_threads);
    }
    for (auto& t : threads) t.join();

    return true;
}

// Write result to output file
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream out(result_path);
    if (!out.is_open()) return false;

    for (const auto& [nation, revenue] : results) {
        out << nation << "|" << std::fixed << std::setprecision(2) << revenue << "\n";
    }

    return true;
}

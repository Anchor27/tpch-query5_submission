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

using namespace std;

// Parse command line arguments
bool parseArgs(int argc, char* argv[], string& r_name, string& start_date, string& end_date,
               int& num_threads, string& table_path, string& result_path) {

    // Initialize defaults
    r_name = "";
    start_date = "";
    end_date = "";
    num_threads = 1;
    table_path = "";
    result_path = "";

    // Parse each argument
    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];

        if (arg == "--r_name" && i + 1 < argc) {
            r_name = argv[++i];
        } else if (arg == "--start_date" && i + 1 < argc) {
            start_date = argv[++i];
        } else if (arg == "--end_date" && i + 1 < argc) {
            end_date = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = stoi(argv[++i]);
        } else if (arg == "--table_path" && i + 1 < argc) {
            table_path = argv[++i];
        } else if (arg == "--result_path" && i + 1 < argc) {
            result_path = argv[++i];
        } else {
            cerr << "Unknown or incomplete argument: " << arg << "\n";
            return false;
        }
    }

    return true;
}

// Read all required TPC-H tables into memory
bool readTPCHData(const string& table_path,
                  vector<map<string, string>>& customer_data,
                  vector<map<string, string>>& orders_data,
                  vector<map<string, string>>& lineitem_data,
                  vector<map<string, string>>& supplier_data,
                  vector<map<string, string>>& nation_data,
                  vector<map<string, string>>& region_data) {

    // Helper function to read a .tbl file
    auto read_tbl = [](const string& filepath, const vector<string>& columns,
                       vector<map<string, string>>& output) {
        ifstream file(filepath);
        string line;

        while (getline(file, line)) {
            stringstream ss(line);
            string token;
            map<string, string> row;
            int column_index = 0;

            while (getline(ss, token, '|') && column_index < (int)columns.size()) {
                row[columns[column_index++]] = token;
            }

            output.push_back(move(row));
        }
    };

    // Read each required file
    read_tbl(table_path + "/region.tbl",   {"regionkey", "name", "comment"}, region_data);
    read_tbl(table_path + "/nation.tbl",   {"nationkey", "name", "regionkey", "comment"}, nation_data);
    read_tbl(table_path + "/customer.tbl", {"custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"}, customer_data);
    read_tbl(table_path + "/orders.tbl",   {"orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment"}, orders_data);
    read_tbl(table_path + "/lineitem.tbl", {"orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount", "tax", "returnflag", "linestatus", "shipdate", "commitdate", "receiptdate", "shipinstruct", "shipmode", "comment"}, lineitem_data);
    read_tbl(table_path + "/supplier.tbl", {"suppkey", "name", "address", "nationkey", "phone", "acctbal", "comment"}, supplier_data);

    return true;
}

// Execute TPC-H Query 5 logic
bool executeQuery5(const string& r_name, const string& start_date, const string& end_date, int num_threads,
                   const vector<map<string, string>>& customer_data,
                   const vector<map<string, string>>& orders_data,
                   const vector<map<string, string>>& lineitem_data,
                   const vector<map<string, string>>& supplier_data,
                   const vector<map<string, string>>& nation_data,
                   const vector<map<string, string>>& region_data,
                   map<string, double>& results) {

    mutex result_mutex;

    // Step 1: Find the region key for the given region name
    string target_regionkey = "";
    for (const auto& row : region_data) {
        if (row.at("name") == r_name) {
            target_regionkey = row.at("regionkey");
            break;
        }
    }
    if (target_regionkey.empty()) {
        return false;
    }

    // Step 2: Collect nation keys belonging to the region
    set<string> target_nationkeys;
    unordered_map<string, string> nationkey_to_name;

    for (const auto& row : nation_data) {
        string nationkey = row.at("nationkey");
        nationkey_to_name[nationkey] = row.at("name");

        if (row.at("regionkey") == target_regionkey) {
            target_nationkeys.insert(nationkey);
        }
    }

    // Step 3: Identify valid suppliers and customers
    set<string> target_suppkeys;
    unordered_map<string, string> suppkey_to_nationkey;

    for (const auto& row : supplier_data) {
        string nationkey = row.at("nationkey");
        string suppkey = row.at("suppkey");

        if (target_nationkeys.count(nationkey)) {
            target_suppkeys.insert(suppkey);
            suppkey_to_nationkey[suppkey] = nationkey;
        }
    }

    set<string> target_custkeys;
    for (const auto& row : customer_data) {
        if (target_nationkeys.count(row.at("nationkey"))) {
            target_custkeys.insert(row.at("custkey"));
        }
    }

    // Step 4: Filter orders by customers and order date
    set<string> target_orderkeys;
    for (const auto& row : orders_data) {
        string orderdate = row.at("orderdate");
        string custkey = row.at("custkey");

        if (target_custkeys.count(custkey) &&
            orderdate >= start_date &&
            orderdate < end_date) {
            target_orderkeys.insert(row.at("orderkey"));
        }
    }

    // Step 5: Parallel revenue aggregation
    auto worker = [&](int tid, int total_threads) {
        map<string, double> local_result;

        for (size_t i = tid; i < lineitem_data.size(); i += total_threads) {
            const auto& row = lineitem_data[i];

            string suppkey = row.at("suppkey");
            string orderkey = row.at("orderkey");

            if (!target_suppkeys.count(suppkey)) continue;
            if (!target_orderkeys.count(orderkey)) continue;

            double extendedprice = stod(row.at("extendedprice"));
            double discount = stod(row.at("discount"));
            double revenue = extendedprice * (1.0 - discount);

            string nationkey = suppkey_to_nationkey[suppkey];
            string nation_name = nationkey_to_name[nationkey];

            local_result[nation_name] += revenue;
        }

        // Merge local result into final result
        lock_guard<mutex> lock(result_mutex);
        for (const auto& pair : local_result) {
            results[pair.first] += pair.second;
        }
    };

    // Launch threads
    vector<thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i, num_threads);
    }
    for (auto& t : threads) {
        t.join();
    }

    return true;
}

// Write final result to file
bool outputResults(const string& result_path, const map<string, double>& results) {
    ofstream out(result_path);
    if (!out.is_open()) {
        return false;
    }

    for (const auto& [nation, revenue] : results) {
        out << nation << "|" << fixed << setprecision(2) << revenue << "\n";
    }

    return true;
}

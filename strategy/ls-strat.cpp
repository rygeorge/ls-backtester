#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <stdexcept>

using namespace std;

class DataFrame {
public:
    vector<vector<double>> data;
    vector<string> headers;

    void addRow(const vector<double>& row) {
        data.push_back(row);
    }

    void print() const {
        for (const auto& row : data) {
            for (const auto& item : row) {
                cout << item << " ";
            }
            cout << endl;
        }
    }
};

bool isNumber(const string& s) {
    try {
        stod(s);
        return true;
    } catch (invalid_argument& e) {
        return false;
    } catch (out_of_range& e) {
        return false;
    }
}

void readCSV(const string& filename, DataFrame& df) {
    ifstream file(filename);

    if (!file.is_open()) {
        cerr << "Could not open the file " << filename << endl;
        return;
    }

    string line;
    bool header = true;
    while (getline(file, line)) {
        stringstream ss(line);
        string item;
        vector<double> row;

        if (header) {
            while (getline(ss, item, ',')) {
                df.headers.push_back(item);
            }
            header = false;
        } else {
            while (getline(ss, item, ',')) {
                if (isNumber(item)) {
                    row.push_back(stod(item));
                } else {
                    row.push_back(numeric_limits<double>::quiet_NaN()); // Use NaN for invalid data
                }
            }
            df.addRow(row);
        }
    }

    file.close();
}

vector<vector<double>> calculateDailyReturns(const DataFrame& df) {
    int rows = df.data.size();
    if (rows <= 1) {
        return vector<vector<double>>(); // Return an empty vector if not enough data
    }
    int cols = df.data[0].size();
    vector<vector<double>> daily_returns(rows - 1, vector<double>(cols));

    for (int i = 1; i < rows; ++i) {
        for (int j = 0; j < cols; ++j) {
            if (!isnan(df.data[i][j]) && !isnan(df.data[i - 1][j])) {
                daily_returns[i - 1][j] = (df.data[i][j] - df.data[i - 1][j]) / df.data[i - 1][j];
            } else {
                daily_returns[i - 1][j] = 0; // Use 0 return if data is invalid
            }
        }
    }

    return daily_returns;
}

vector<vector<int>> rankData(const vector<vector<double>>& daily_returns) {
    int rows = daily_returns.size();
    if (rows == 0) {
        return vector<vector<int>>(); // Return an empty vector if no data
    }
    int cols = daily_returns[0].size();
    vector<vector<int>> rank_matrix(rows, vector<int>(cols));

    for (int i = 0; i < rows; ++i) {
        vector<pair<double, int>> daily_returns_with_index(cols);
        for (int j = 0; j < cols; ++j) {
            daily_returns_with_index[j] = make_pair(daily_returns[i][j], j);
        }
        sort(daily_returns_with_index.begin(), daily_returns_with_index.end(), greater<pair<double, int>>());

        for (int j = 0; j < cols; ++j) {
            rank_matrix[i][daily_returns_with_index[j].second] = j + 1;
        }
    }

    return rank_matrix;
}

vector<vector<int>> generateSignals(const vector<vector<int>>& rank_matrix, int threshold) {
    int rows = rank_matrix.size();
    if (rows == 0) {
        return vector<vector<int>>(); // Return an empty vector if no data
    }
    int cols = rank_matrix[0].size();
    vector<vector<int>> signal_matrix(rows, vector<int>(cols, 1));

    for (int i = 0; i < rows; ++i) {
        for (int j = 0; j < cols; ++j) {
            if (rank_matrix[i][j] < threshold) {
                signal_matrix[i][j] = -1;
            }
        }
    }

    return signal_matrix;
}

vector<vector<double>> calculateReturns(const vector<vector<int>>& signal_matrix, const vector<vector<double>>& daily_returns) {
    int rows = signal_matrix.size();
    if (rows == 0) {
        return vector<vector<double>>(); // Return an empty vector if no data
    }
    int cols = signal_matrix[0].size();
    vector<vector<double>> returns_matrix(rows, vector<double>(cols));

    for (int i = 0; i < rows - 1; ++i) {
        for (int j = 0; j < cols; ++j) {
            returns_matrix[i][j] = signal_matrix[i][j] * daily_returns[i + 1][j];
        }
    }

    return returns_matrix;
}

int main() {
    string filename = "/Users/ryangeorge/GitHub Projects/ls-backtester/test-data/financial_data.csv";
    DataFrame df;
    readCSV(filename, df);

    vector<vector<double>> daily_returns = calculateDailyReturns(df);
    vector<vector<int>> rank_matrix = rankData(daily_returns);

    int threshold = 38; // Given the number of stocks @todo change to dynamic based on .size()-1
    vector<vector<int>> signal_matrix = generateSignals(rank_matrix, threshold);

    vector<vector<double>> returns_matrix = calculateReturns(signal_matrix, daily_returns);

    int num_tickers = df.headers.size();
    vector<double> strategy_returns(returns_matrix.size());

    for (int i = 0; i < returns_matrix.size(); ++i) {
        strategy_returns[i] = accumulate(returns_matrix[i].begin(), returns_matrix[i].end(), 0.0) / num_tickers;
    }

    vector<double> cumulative_returns(strategy_returns.size(), 1.0); // Initialize with 1 for cumulative product
    for (int i = 1; i < strategy_returns.size(); ++i) {
        cumulative_returns[i] = cumulative_returns[i - 1] * (1 + strategy_returns[i]);
    }

    double daily_rf_rate = 0;
    double annual_rf_rate = daily_rf_rate * 2386; // @todo dynamically calculate number of days between
    double mean_return = accumulate(strategy_returns.begin(), strategy_returns.end(), 0.0) / strategy_returns.size();
    double variance = 0.0;
    for (const auto& r : strategy_returns) {
        variance += (r - mean_return) * (r - mean_return);
    }
    variance /= strategy_returns.size();
    double strategy_volatility = sqrt(variance) * sqrt(2386);
    double sharpe_ratio = (mean_return - annual_rf_rate) / strategy_volatility;

    vector<double> cum_max(cumulative_returns.size());
    partial_sum(cumulative_returns.begin(), cumulative_returns.end(), cum_max.begin(), [](double a, double b) { return max(a, b); });
    vector<double> drawdown(cumulative_returns.size());
    transform(cumulative_returns.begin(), cumulative_returns.end(), cum_max.begin(), drawdown.begin(), [](double a, double b) { return (a - b) / b; });
    double max_drawdown = *min_element(drawdown.begin(), drawdown.end());

    cout << "Cumulative Returns:" << endl;
    if (!cumulative_returns.empty()) {
        cout << cumulative_returns.back() << endl;
    } else {
        cout << "No trades executed." << endl;
    }

    cout << "\nSharpe Ratio:" << endl;
    cout << sharpe_ratio << endl;

    cout << "\nMax Drawdown:" << endl;
    cout << max_drawdown << endl;

    return 0;
};

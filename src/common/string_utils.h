#pragma once

#include <string>
#include <algorithm>
#include <cctype>

namespace tinydb {
namespace common {

/**
 * Convert a string to lowercase
 */
inline std::string toLower(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

} // namespace common
} // namespace tinydb

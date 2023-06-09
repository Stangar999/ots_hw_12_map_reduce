#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

#include "map_reduce.h"

using namespace std::literals;

/// Считаем длину общего префикса у строк
size_t string_diff(const std::string& lhv, const std::string& rhv) {
  size_t length = std::min(lhv.size(), rhv.size());
  for (size_t i = 0; i < length; i++) {
    if (lhv[i] != rhv[i])
      return i + 1;
  }
  return length;
}

std::vector<std::string> mapper(const std::string& paragraph) {
  std::stringstream ss(paragraph);
  std::string str;
  std::vector<std::string> result;
  while (getline(ss, str)) {
    result.push_back(std::move(str));
  }
  return result;
}

/// записывает слово и длину минимального пркфикса для него
void reduser(std::vector<std::string> lines) {
  size_t result = 0;
  size_t prev_result = 0;
  std::string prev;
  static int count = 0;
  std::ofstream fs("reducer_" + std::to_string(++count) + ".txt");

  for (const auto& line : lines) {
    if (!prev.empty()) {
      result = string_diff(line, prev);
      if (prev_result != 0) {
        fs << prev << " " << std::max(prev_result, result) << std::endl;
      } else {
        fs << prev << " " << result << std::endl;
      }
      prev_result = result;
    }
    prev = line;
  }
  if (!prev.empty()) {
    if (prev_result != 0) {
      fs << prev << " " << result << std::endl;
    } else {
      fs << prev << " " << 1 << std::endl;
    }
  }
}

int main(int argc, const char* argv[]) {
  if (argc == 4) {
    size_t map_threads = atoi(argv[2]);
    size_t reduce_threads = atoi(argv[3]);

    if (map_threads == 0) {
      std::cout << "Map threads should be greater then zero" << std::endl;
      return 0;
    }

    if (reduce_threads == 0) {
      std::cout << "Reduce threads should be greater then zero" << std::endl;
      return 0;
    }

    //"C:/Users/locki/Project/Otus/ots_hw_12_mapreduce/src/test.txt"sv
    std::filesystem::path p(argv[1], std::filesystem::path::generic_format);

    MapReduce map_reduce(p, map_threads, reduce_threads, mapper, reduser);
  } else {
    std::cerr << "Usage: mapreduce <src> <mnum> <rnum>\n"
              << "src  – путь к файлу с данными.\n"
              << "mnum – количество потоков для работы отображения\n"
              << "rnum – количество потоков для работы свертки\n";
  }
  return 0;
}

// версия с разделением по буквам как обьясняли в лекции
// но получившиеся уникальные префиксы надо сложить в один контейнер
// что бы удалить слова с этими префиксами из дальнейшей обработки
// что нарушает идею что работаем с BigData
// поэтому как применить этот подход я не придумал

//#include <iostream>
//#include <fstream>
//#include <filesystem>
//#include <cmath>
//#include <algorithm>
//#include <thread>
//#include <sstream>
//#include <future>

// const size_t kCountThread = 4;
// const char kChar_n =  '\n';
// using namespace std::literals;

// std::string mapper(const std::string& line, size_t level) {
//   return line.substr(0, level);
// }

// std::vector<std::string> reduser(std::vector<std::string> lines) {
//   std::vector<std::string> uniq_pref;
//   if(lines.empty()){
//     return {};
//   }
//   std::string prev_line = lines.front();
//   size_t counter = 0;
//   for(const auto& line : lines) {
//     if(prev_line != line) {
//       if(counter == 1) {
//         uniq_pref.push_back(prev_line);
//       }
//       prev_line = line;
//       counter = 0;
//     }
//     ++counter;
//   }
//   return uniq_pref;
// }

// class MapReduce {
//   struct MapHolder {
//     std::vector<std::string> map_results;
//     std::vector<std::string>::iterator it = map_results.begin();
//   };

// public:
//   MapReduce(std::filesystem::path p, size_t M, size_t R)
//     : _p(p)
//     , _M(M)
//     , _R(R)
//     , _level(0)
//   {

//    std::vector<std::pair<size_t, size_t>> ranges = split();

//    bool isRepeted = true;
//    while(isRepeted) {
//      ++_level;
//      isRepeted = false;

//      // запуск мапинга
//      std::vector<std::future<std::vector<std::string>>> futures;
//      std::vector<std::thread> worckers_map;
//      worckers_map.reserve(ranges.size());
//      for(const auto& var : ranges) {
//        std::promise<std::vector<std::string>> accumulate_promise;
//        futures.push_back(accumulate_promise.get_future());
//        worckers_map.emplace_back(&MapReduce::ExtractRange, this,
//        std::cref(var), std::move(accumulate_promise));
//      }
//      for(std::thread& worcker : worckers_map) {
//        worcker.join();
//      }

//      // запись результатов мапинга
//      size_t common_size_map_result = 0;
//      std::vector<MapHolder> map_holder_result;
//      for(auto& fut : futures) {
//        map_holder_result.push_back({fut.get()});
//        // подсчет общего колличества данных для редьюса
//        common_size_map_result += map_holder_result.back().map_results.size();
//      }

//      // создание контейнеров редьюс
//      std::vector<std::vector<std::string>> redusers(_R);
//      auto cur_reduser_it = redusers.begin();
//      // расчет примерного количества данных на каждый контейнр редьюс
//      size_t size_path_redus = std::ceil(common_size_map_result * 1. / _R);

//      // реализация shuffle пока все данные не перемещены
//      std::string prev_min_str;
//      while(!map_holder_result.empty()) {

//        // поиск минимальной строки среди всех результатов мапинга
//        std::string cur_min_str = *map_holder_result.back().it;
//        for(auto& [map_result, it] : map_holder_result) {
//          const std::string& str = *it;
//          if(str > prev_min_str && str < cur_min_str) {
//            cur_min_str = str;
//          }
//        }

//        prev_min_str = cur_min_str;
//        // находим все строки равные cur_min_str и записываем их
//        // в один редьюс контейнер (cur_reduser_it итератор на текущий
//        контейнер std::string cur_str = cur_min_str; for(auto& [map_result,
//        it] : map_holder_result) {
//          for(;it != map_result.end(); ++it) {
//            std::string& str = *it;
//            if(str == cur_str) {
//              cur_reduser_it->push_back(std::move(str));
//            } else if (str > cur_str) {
//              break;
//            }
//          }
//        }

//        // если размер текушего  редьюс контейнера больше предпологаемого
//        среднего
//        // то переходим к следующему если он не последний конечно
//        if(cur_reduser_it->size() > size_path_redus &&
//        std::next(cur_reduser_it) != redusers.end()) {
//          ++cur_reduser_it;
//        }

//        // обходим все мап контейнеры и удаляем те которые полностью обошли
//        // текущая позиция в контейнере хранится в struct MapHolder
//        for(auto it_map_holder = map_holder_result.begin();
//            it_map_holder != map_holder_result.end();)
//        {
//          if(it_map_holder->it == it_map_holder->map_results.end()) {
//            it_map_holder = map_holder_result.erase(it_map_holder);
//          } else {
//            ++it_map_holder;
//          }
//        }
//      }

//      // запуск редьюсинга
//      std::vector<std::future<int>> futures_r;
//      std::vector<std::thread> worckers_red;
//      worckers_red.reserve(_R);
//      for(const std::vector<std::string>& lines : redusers) {
//        std::promise<int> accumulate_promise;
//        futures_r.push_back(accumulate_promise.get_future());
//        worckers_red.emplace_back(&MapReduce::ApplyReduce, this,
//        std::cref(lines), std::move(accumulate_promise));
//      }
//      for(std::thread& worcker : worckers_red) {
//        worcker.join();
//      }
//      // проверяем были ли повторения
//      for(auto& fut : futures_r) {
//        if(fut.get()) {
//          isRepeted = true;
//        }
//      }
//    }
//    int a = 0;
//  }

// private:
//   // разбиение файла по колличеству маперов
//   std::vector<std::pair<size_t, size_t>> split() {
//     std::fstream fs(_p, std::ios_base::in | std::ios_base::binary);
//     std::filesystem::directory_entry d_e(_p);
//     size_t size_path = std::ceil(d_e.file_size() * 1. / _M);
//     std::cout << d_e.file_size() << std::endl;
//     std::cout << size_path << std::endl;

//    std::vector<std::pair<size_t, size_t>> ranges;
//    size_t cur_pos = 0;
//    size_t prev_pos = 0;
//    while(!fs.eof()) {
//      fs.seekg(cur_pos + size_path);
//      std::string str;
//      std::getline(fs, str);
//      size_t new_pos = fs.tellg();

//      if(fs.eof()) {
//        ranges.push_back({prev_pos, d_e.file_size()});
//        break;
//      }
//      ranges.push_back({prev_pos, new_pos});
//      prev_pos = fs.tellg();
//      cur_pos += size_path;
//    }
//    return ranges;
//  }

//  // -------------- функции мапинга------------------------
//  std::vector<std::string> SpliteRangeOnLine(const std::string& path) {
//    std::stringstream ss(path);
//    std::string str;
//    std::vector<std::string> result;
//    while(getline(ss, str)) {
//       result.push_back(std::move(str));
//    }
//    return result;
//  }

//  // извлекает диапозон из файла в строку
//  void ExtractRange(std::pair<size_t, size_t> range,
//  std::promise<std::vector<std::string>> accumulate_promise) {
//    std::string str_range;
//    size_t read_size = range.second - range.first;
//    str_range.resize(read_size);
//    std::fstream fs(_p, std::ios_base::in);
//    fs.seekg(range.first);
//    fs.read(str_range.data(), read_size);
//    // из абзаца делает вектор строк
//    std::vector<std::string> lines = SpliteRangeOnLine(str_range);
//    // применяет маппер к каждой строке и возвращает вектор результирующих
//    строк std::vector<std::string> data_prepare =
//    ApplyMaper(std::move(lines)); std::sort(data_prepare.begin(),
//    data_prepare.end());
//    accumulate_promise.set_value(std::move(data_prepare));
//  }

//  std::vector<std::string> ApplyMaper(std::vector<std::string> lines) {
//    std::vector<std::string> data_prepare;
//    for(const auto& line : lines) {
//      data_prepare.push_back(mapper(line, _level));
//    }
//    return data_prepare;
//  }

//  // -------------- функции редьюса------------------------
//  void ApplyReduce(std::vector<std::string> lines, std::promise<int>
//  accumulate_promise) {
//    std::vector<std::string> uniq_pref;
//    uniq_pref = reduser(lines);
//    bool isRepet = uniq_pref.size() != lines.size();
//    accumulate_promise.set_value(isRepet);
//  }

//  std::filesystem::path _p;
//  size_t _M;
//  size_t _R;
//  size_t _level;

//};

// int main(int argc, const char* argv[]) {
//   std::filesystem::path
//   p("C:/Users/locki/Project/Otus/ots_hw_12_mapreduce/src/test.txt"sv,
//                           std::filesystem::path::generic_format);
//   MapReduce map_reduce(p, kCountThread, kCountThread);
//   return 0;
// }

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <sstream>
#include <thread>

template <typename Maper, typename Reducer>
class MapReduce {
  struct MapHolder {
    std::vector<std::string> map_results;
    std::vector<std::string>::iterator it = map_results.begin();
  };

 public:
  /// @brief – путь к файлу с исходными данными
  /// количество потоков для работы отображения
  /// количество потоков для работы свертки
  /// функция мапинга
  /// функция рельюса
  MapReduce(std::filesystem::path p, size_t M, size_t R, Maper func_maper,
            Reducer func_reduce)
      : _p(p),
        _M(M),
        _R(R),
        _func_maper(func_maper),
        _func_reduce(func_reduce)

  {
    std::vector<std::pair<size_t, size_t>> ranges = split();

    // запуск мапинга
    std::vector<std::future<std::vector<std::string>>> futures;
    std::vector<std::thread> worckers_map;
    worckers_map.reserve(ranges.size());
    for (const auto& var : ranges) {
      std::promise<std::vector<std::string>> accumulate_promise;
      futures.push_back(accumulate_promise.get_future());
      worckers_map.emplace_back(&MapReduce::ApplyMaper, this, std::cref(var),
                                std::move(accumulate_promise));
    }
    for (std::thread& worcker : worckers_map) {
      worcker.join();
    }

    // запись результатов мапинга
    size_t common_size_map_result = 0;
    std::vector<MapHolder> map_holder_result;
    for (auto& fut : futures) {
      map_holder_result.push_back({fut.get()});
      // подсчет общего колличества данных для редьюса
      common_size_map_result += map_holder_result.back().map_results.size();
    }

    // создание контейнеров редьюс
    std::vector<std::vector<std::string>> redusers(_R);
    auto cur_reduser_it = redusers.begin();
    // расчет примерного количества данных на каждый контейнр редьюс
    size_t size_path_redus = std::ceil(common_size_map_result * 1. / _R);

    // реализация shuffle пока все данные не перемещены
    char prev_min_c;
    while (!map_holder_result.empty()) {
      // поиск минимальной строки среди всех результатов мапинга
      char cur_min_c = map_holder_result.back().it->front();
      for (auto& [map_result, it] : map_holder_result) {
        char c = it->front();
        if (c > prev_min_c && c < cur_min_c) {
          cur_min_c = c;
        }
      }

      prev_min_c = cur_min_c;
      // находим все строки начинающиеся с cur_min и записываем их
      // в один редьюс контейнер (cur_reduser_it итератор на текущий контейнер
      char cur_c = cur_min_c;
      for (auto& [map_result, it] : map_holder_result) {
        for (; it != map_result.end(); ++it) {
          std::string& str = *it;
          char c = str.front();
          if (c == cur_c) {
            cur_reduser_it->push_back(std::move(str));
          } else if (c > cur_c) {
            break;
          }
        }
      }

      // если размер текушего  редьюс контейнера больше предпологаемого среднего
      // то переходим к следующему если он не последний конечно
      if (cur_reduser_it->size() > size_path_redus &&
          std::next(cur_reduser_it) != redusers.end()) {
        // сортируем микс из разных маперов
        std::sort(cur_reduser_it->begin(), cur_reduser_it->end());
        ++cur_reduser_it;
      }

      // обходим все мап контейнеры и удаляем те которые полностью обошли
      // текущая позиция в контейнере хранится в struct MapHolder
      for (auto it_map_holder = map_holder_result.begin();
           it_map_holder != map_holder_result.end();) {
        if (it_map_holder->it == it_map_holder->map_results.end()) {
          it_map_holder = map_holder_result.erase(it_map_holder);
        } else {
          ++it_map_holder;
        }
      }
    }

    // запуск редьюсинга
    std::vector<std::thread> worckers_red;
    worckers_red.reserve(_R);
    for (const std::vector<std::string>& lines : redusers) {
      worckers_red.emplace_back(&MapReduce::ApplyReduce, this,
                                std::cref(lines));
    }
    for (std::thread& worcker : worckers_red) {
      worcker.join();
    }
  }

 private:
  /// @brief разбиение файла по колличеству маперов
  std::vector<std::pair<size_t, size_t>> split() {
    std::fstream fs(_p, std::ios_base::in | std::ios_base::binary);
    std::filesystem::directory_entry d_e(_p);
    size_t size_path = std::ceil(d_e.file_size() * 1. / _M);
    std::cout << d_e.file_size() << std::endl;
    std::cout << size_path << std::endl;

    std::vector<std::pair<size_t, size_t>> ranges;
    size_t cur_pos = 0;
    size_t prev_pos = 0;
    while (!fs.eof()) {
      fs.seekg(cur_pos + size_path);
      std::string str;
      std::getline(fs, str);
      size_t new_pos = fs.tellg();

      if (fs.eof()) {
        ranges.push_back({prev_pos, d_e.file_size()});
        break;
      }
      ranges.push_back({prev_pos, new_pos});
      prev_pos = fs.tellg();
      cur_pos += size_path;
    }
    return ranges;
  }

  /// @brief извлекает диапозон из файла в строку и применяем маппер потом
  /// сортируем
  void ApplyMaper(std::pair<size_t, size_t> range,
                  std::promise<std::vector<std::string>> accumulate_promise) {
    std::string str_range;
    size_t read_size = range.second - range.first;
    str_range.resize(read_size);
    std::fstream fs(_p, std::ios_base::in);
    fs.seekg(range.first);
    fs.read(str_range.data(), read_size);
    std::vector<std::string> data_prepare = _func_maper(str_range);
    std::sort(data_prepare.begin(), data_prepare.end());
    accumulate_promise.set_value(std::move(data_prepare));
  }

  /// @brief редьюс
  void ApplyReduce(std::vector<std::string> lines) {
    if (lines.empty()) {
      return;
    }
    _func_reduce(lines);
  }

  std::filesystem::path _p;
  size_t _M;
  size_t _R;
  Maper _func_maper;
  Reducer _func_reduce;
};

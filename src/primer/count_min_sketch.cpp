//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <stdexcept>
#include <string>
#include <algorithm>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : 
    width_(width), 
    depth_(depth)
{
  /** @TODO(student) Implement this function! */
  if(width == 0 || depth == 0) {
    throw std::invalid_argument("invalid width or depth");
  }
  table_.reserve(depth_);
  for (uint32_t i = 0; i < depth_; ++i) {
    table_.emplace_back(width_);  // 在容器内原地构造 inner vector(size_t)
    // inner vector 会 value-initialize 它的 atomic 元素（即为 0）
  }

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : 
    width_(other.width_), 
    depth_(other.depth_),
    hash_functions_(std::move(other.hash_functions_)),
    table_(std::move(other.table_)) 
{
  /** @TODO(student) Implement this function! */
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if(this == &other) {
    return *this;
  }
  depth_ = other.depth_;
  width_ = other.width_;
  hash_functions_ = std::move(other.hash_functions_);
  table_ = std::move(other.table_);
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  for(uint32_t i = 0; i < depth_; i++) {
    size_t index = hash_functions_[i](item);
    table_[i][index].fetch_add(1, std::memory_order_relaxed); 
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  for(uint32_t i = 0; i < depth_; i++) {
    for(uint32_t j = 0; j < width_; j++) {
      table_[i][j].fetch_add(other.table_[i][j].load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t res = UINT32_MAX;
  for(uint32_t i = 0; i < depth_; i++) {
    size_t index = hash_functions_[i](item);
    res = std::min(table_[i][index].load(std::memory_order_relaxed), res);
  }
  return res;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  for(uint32_t i = 0; i < depth_; i++) {
    for(uint32_t j = 0; j < width_; j++) {
      table_[i][j].store(0, std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  std::vector<std::pair<KeyType, uint32_t>> res;
  size_t size = candidates.size();
  res.resize(size);
  for(auto &it : candidates) {
    res.emplace_back(it, Count(it));
  }
  if(k > size) {
    k = size;
  }
  std::partial_sort(res.begin(), res.begin() + k, res.end(), 
        [](const std::pair<KeyType, uint32_t>& a, const std::pair<KeyType, uint32_t>& b) {
            return a.second > b.second;  // 第二个元素：count
        });
  res.resize(k);
  /** @TODO(student) Implement this function! */
  return res;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub

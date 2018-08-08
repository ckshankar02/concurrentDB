/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INVALID_INDEX -1

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *bpm, page_id_t pg_id);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  //IndexIterator &operator++();
  INDEXITERATOR_TYPE &operator++();

private:
  // add your own private member variables here
	BufferPoolManager *buffer_pool_manager;
	//B_PLUS_TREE_LEAF_PAGE_TYPE *current_page;
	page_id_t current_page_id;
	int64_t current_index; 
};

} // namespace cmudb

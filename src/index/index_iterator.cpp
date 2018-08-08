/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
//INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm,
//																	B_PLUS_TREE_LEAF_PAGE_TYPE *current_pg) 
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id) 
{
		B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_pg; 
		this->buffer_pool_manager = bpm;
		this->current_page_id = page_id;

		leaf_pg = (B_PLUS_TREE_LEAF_PAGE_TYPE *)this->buffer_pool_manager->FetchPage
																													(this->current_page_id);
		this->current_index = (leaf_pg->GetSize())?0:INVALID_INDEX;
		this->buffer_pool_manager->UnpinPage(leaf_pg->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd()
{
	if(this->current_index == INVALID_INDEX)
		return true;
	return false;
}



INDEX_TEMPLATE_ARGUMENTS
const MappingType& INDEXITERATOR_TYPE::operator*()
{
		B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_pg; 
		leaf_pg = (B_PLUS_TREE_LEAF_PAGE_TYPE *)this->buffer_pool_manager->FetchPage
																											(this->current_page_id);
		auto *item = &leaf_pg->GetItem(this->current_index); 
		this->buffer_pool_manager->UnpinPage(leaf_pg->GetPageId(),false);
		return *item;
}


//IndexIterator& INDEXITERATOR_TYPE::operator++()
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE& INDEXITERATOR_TYPE::operator++()
{
	B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_pg;

	if(this->current_page_id == INVALID_PAGE_ID)
		return *this;	
 
	leaf_pg = (B_PLUS_TREE_LEAF_PAGE_TYPE *)this->buffer_pool_manager->FetchPage
																												(this->current_page_id);
	if(this->current_index < leaf_pg->GetSize()-1)
	{
			this->current_index++;
	}
	else if(leaf_pg->GetNextPageId() != INVALID_PAGE_ID)
	{
			page_id_t next_pg_id = leaf_pg->GetNextPageId();
			this->current_index = 0;
			this->current_page_id = next_pg_id;
	}
	else
	{
		//this->buffer_pool_manager->UnpinPage(leaf_pg->GetPageId(), false);
			this->current_page_id = INVALID_PAGE_ID;
			this->current_index = INVALID_INDEX;
	}
	this->buffer_pool_manager->UnpinPage(leaf_pg->GetPageId(), false);
	return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb

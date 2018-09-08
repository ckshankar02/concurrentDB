/*
 * b_plus_tree.cpp
 */
#include <iostream>
#include <queue>
#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name, 
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const 
{ 
  if(this->root_page_id_ == INVALID_PAGE_ID) return true;
  return false; 
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) 
{
    bool res = false;
    BPlusTreePage *page_ptr;
		std::shared_lock<std::mutex> *shr_lck;

		if(this->IsEmpty()) return false;

		page_ptr = (BPlusTreePage *)this->buffer_pool_manager_->
																FetchPage(this->root_page_id_);
    page_id_t pg_id;
    ValueType value;	

		std::stack<std::shared_lock<std::mutex> *> lock_stack;
		shr_lock = new std::shared_lock<std::mutex>(page_ptr->page_mtx);
		lock_stack.push(shr_lock);
	
	  while(!page_ptr->IsLeafPage())
    {
        // value is page id here
        pg_id = ((B_PLUS_TREE_INTERNAL_PG_PGID *)page_ptr)->Lookup
                                                 (key, this->comparator_);
        this->buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);

        page_ptr = 
              (BPlusTreePage *)this->buffer_pool_manager_->FetchPage(pg_id);
				
				shr_lock = new std::shared_lock<std::mutex>(page_ptr->page_mtx);

				lock_stack.top()->unlock();
				lock_stack.pop();
				lock_stack.push(shr_lock);
    }

    if(((B_PLUS_TREE_LEAF_PAGE_TYPE *)page_ptr)->Lookup
                                (key, value, this->comparator_))
    {
        result.push_back(value);
        res = true;
    }

    this->buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                                    Transaction *transaction)
{
    /* Current tree is empty */
    if(this->IsEmpty())
    {
      this->StartNewTree(key, value);
      return true;
    }
    
    return this->InsertIntoLeaf(key, value, transaction); 
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) 
{
    page_id_t root_page_id;

    B_PLUS_TREE_LEAF_PAGE_TYPE *root_page = 
        (B_PLUS_TREE_LEAF_PAGE_TYPE *)this->buffer_pool_manager_->NewPage
                                                                (root_page_id);
    if(root_page == nullptr) 
        throw "Out of memory!";

    this->root_page_id_ = root_page_id;
    root_page->Init(this->root_page_id_, INVALID_PAGE_ID);

    this->UpdateRootPageId(true);

    root_page->Insert(key, value, this->comparator_);
  
    this->buffer_pool_manager_->UnpinPage(this->root_page_id_, true);

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) 
{
    KeyType tmp_key;
    ValueType tmp_value;
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_pg = this->FindLeafPage(key, false);

    /* Key already exists. Trying to insert duplicate key*/
    if(leaf_pg->Lookup(key, tmp_value, this->comparator_))
    {
        this->buffer_pool_manager_->UnpinPage(leaf_pg->GetPageId(),false);
        return false;
    }

    /* Page/Node is not full */
    if(leaf_pg->GetSize() < leaf_pg->GetMaxSize())
        leaf_pg->Insert(key, value, this->comparator_);
    else
    {
    		B_PLUS_TREE_LEAF_PAGE_TYPE *sib_leaf_pg = nullptr;
 
	       /* Node is max'ed out, need to split */
        sib_leaf_pg = this->Split(leaf_pg);

        tmp_key = sib_leaf_pg->KeyAt(0);

        if(this->comparator_(tmp_key, key) < 0)
            sib_leaf_pg->Insert(key, value, this->comparator_);
        else
            leaf_pg->Insert(key, value, this->comparator_);

        this->InsertIntoParent(leaf_pg, tmp_key, sib_leaf_pg, transaction);  
        this->buffer_pool_manager_->UnpinPage(sib_leaf_pg->GetPageId(), true);
    }

    this->buffer_pool_manager_->UnpinPage(leaf_pg->GetPageId(), true);
    return true; 
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) 
{
    page_id_t pg_id;
    page_id_t parent_pg_id = node->GetParentPageId();

    N *bptree_pg = (N *) this->buffer_pool_manager_->NewPage(pg_id);
		assert(pg_id != INVALID_PAGE_ID);

    if(bptree_pg == nullptr){ 
				std::cout<<"Out of Memory\n";
        throw "Out of memory!";
		}

    bptree_pg->Init(pg_id, parent_pg_id);
    node->MoveHalfTo(bptree_pg, this->buffer_pool_manager_);

/*    if(node->GetPageType == LEAF_PAGE)
    {
      BPlusTreeLeafPage *leaf_pg = ((BPlusTreeLeafPage *)bt_pg);
      leaf_pg->Init(pg_id, parent_pg_id);
      node->MoveHalfTo(leaf_pg, this->buffer_pool_manager_);
    }
    else
    {
      BPlusTreeInternalPage *int_pg = ((BPlusTreeInternalPage *)bt_pg);
      int_pg->Init(pg_id, parent_pg_id);
      node->MoveHalfTo(int_pg, this->buffer_pool_manager_);
    }
*/
		if(node->IsLeafPage())
		{
				page_id_t next_page_id = ((B_PLUS_TREE_LEAF_PAGE_TYPE *)node)->GetNextPageId();
				((B_PLUS_TREE_LEAF_PAGE_TYPE *)node)->SetNextPageId(bptree_pg->GetPageId());
				((B_PLUS_TREE_LEAF_PAGE_TYPE *)bptree_pg)->SetNextPageId(next_page_id);
		}
    return bptree_pg; 
}

INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_INTERNAL_PG_PGID* BPLUSTREE_TYPE::GetNewRoot()
{
    page_id_t root_pgid = 0;
    BufferPoolManager *bpm = this->buffer_pool_manager_;

    B_PLUS_TREE_INTERNAL_PG_PGID *new_root_pg = 
													(B_PLUS_TREE_INTERNAL_PG_PGID *)bpm->NewPage(root_pgid);
		assert(root_pgid != INVALID_PAGE_ID);
    new_root_pg->Init(root_pgid, NO_PARENT);
    this->root_page_id_ = root_pgid;
    this->UpdateRootPageId(false);

    return new_root_pg;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustNextPageId(BPlusTreePage *new_pg, 
													  B_PLUS_TREE_INTERNAL_PG_PGID *parent_pg)
{
		if(new_pg->IsLeafPage())
		{
				int64_t NodeIndex = parent_pg->ValueIndex(new_pg->GetPageId());
				if(NodeIndex+1 < parent_pg->GetSize())
				{
					
						((B_PLUS_TREE_LEAF_PAGE_TYPE *)new_pg)->SetNextPageId
																				(parent_pg->ValueAt(NodeIndex+1));
				}
		}
} 

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) 
{ 
    B_PLUS_TREE_INTERNAL_PG_PGID *parent_pg;

    if(old_node->IsRootPage())
    {
			parent_pg = this->GetNewRoot();
			old_node->SetParentPageId(parent_pg->GetPageId());
			new_node->SetParentPageId(parent_pg->GetPageId());

			parent_pg->PopulateNewRoot(old_node->GetPageId(), key, 
																											new_node->GetPageId());
      this->buffer_pool_manager_->UnpinPage(this->root_page_id_, true);
			return;	
    }
    else	
     	parent_pg = 
      	 (B_PLUS_TREE_INTERNAL_PG_PGID *)this->buffer_pool_manager_->FetchPage
                                                (old_node->GetParentPageId());
	
    /* Enough space left in parent */
    if(parent_pg->GetSize() < parent_pg->GetMaxSize())
    {
        parent_pg->InsertNodeAfter(old_node->GetPageId(), key, 
                                         new_node->GetPageId());
    }
    else /* Not enough space left. Split required */
    {
        page_id_t tmp_value;
        B_PLUS_TREE_INTERNAL_PG_PGID *sib_pg = this->Split(parent_pg);
        KeyType tmp_key = sib_pg->KeyAt(0);

        /* Insert into parent or parent's new sibbling */
        if(this->comparator_(tmp_key, key) < 0)
        {
            /* Lookup the index of the key/value pair that might
             * lead to the page containing the 'key'.*/
            tmp_value = sib_pg->Lookup(key, this->comparator_);
            sib_pg->InsertNodeAfter(tmp_value, key, new_node->GetPageId());
						new_node->SetParentPageId(sib_pg->GetPageId());
						//this->AdjustNextPageId(new_node, sib_pg);
        }
        else
        {
            /* Lookup the index of the key/value pair that might
             * lead to the page containing the 'key'.*/
            tmp_value = parent_pg->Lookup(key, this->comparator_);
            parent_pg->InsertNodeAfter(tmp_value, key, new_node->GetPageId());
						new_node->SetParentPageId(parent_pg->GetPageId());
						//this->AdjustNextPageId(new_node, parent_pg);
        }
        
        if(parent_pg->IsRootPage())
        {
            B_PLUS_TREE_INTERNAL_PG_PGID *new_root_pg = this->GetNewRoot(); 

            parent_pg->SetParentPageId(this->root_page_id_);
            sib_pg->SetParentPageId(this->root_page_id_);

            new_root_pg->PopulateNewRoot(parent_pg->GetPageId(), 
                                         tmp_key, sib_pg->GetPageId());

            this->buffer_pool_manager_->UnpinPage(this->root_page_id_, true);
        }
        else 
        {
            this->InsertIntoParent(parent_pg, tmp_key, sib_pg, transaction);
        }

        this->buffer_pool_manager_->UnpinPage(sib_pg->GetPageId(), true);
    }


    this->buffer_pool_manager_->UnpinPage(parent_pg->GetPageId(), true);
}



/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) 
{
    if(this->IsEmpty())
        return;
    
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_pg = this->FindLeafPage(key, false);

		if(leaf_pg == nullptr) return;

    leaf_pg->RemoveAndDeleteRecord(key, this->comparator_);	

    /*if(leaf_pg->GetSize() < leaf_pg->GetMinSize())
    {
        if(this->CoalesceOrRedistribute(leaf_pg, transaction))
        {*/
            /* Unlikely to hit this condition, as the deletion is already
             * taken care of in the CoalesceOrRedistribute()*/
           /* this->buffer_pool_manager_->UnpinPage(leaf_pg->GetPageId(), true);
            this->buffer_pool_manager_->DeletePage(leaf_pg->GetPageId());
						return; 
        }
    }*/

   	if(leaf_pg->GetSize() < leaf_pg->GetMinSize() && 
			 this->CoalesceOrRedistribute(leaf_pg, transaction))
		{
				return;
		}		
    this->buffer_pool_manager_->UnpinPage(leaf_pg->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction)
{
		bool result = false;
		bool parent_deleted = false;
    B_PLUS_TREE_INTERNAL_PG_PGID *parent; 

    if(node->IsRootPage())
    {
				if(node->IsLeafPage()) 
				{
					if(node->GetSize() < 1)
					{
							this->buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
							this->buffer_pool_manager_->DeletePage(node->GetPageId());
							return true;
					}
					return false;
				}				


				if(node->GetSize() > 1)
					return false;


				return AdjustRoot(node);
    }  

		parent = (B_PLUS_TREE_INTERNAL_PG_PGID *)this->buffer_pool_manager_->
																				FetchPage(node->GetParentPageId());

    /* Check posibility of Coalescing */
    int rd_sib_idx = -1; //Sibbling index when redistributing
    int parent_index = parent->ValueIndex(node->GetPageId());

    int sib_index = this->CheckMergeSibbling(parent_index, parent, 
                                             node->GetSize(), 
                                             node->GetMaxSize(), 
                                             rd_sib_idx);

    if(sib_index != INVALID_INDEX)
    {
       N *sib_pg = (N *)this->buffer_pool_manager_->FetchPage
                                            (parent->ValueAt(sib_index));
			
       if(sib_index < parent_index)
			 {
          parent_deleted = this->Coalesce(sib_pg, node, parent, 
																					parent_index, transaction);
					this->buffer_pool_manager_->UnpinPage(sib_pg->GetPageId(), true);
					result = true;
			 }
       else
			 { 
          parent_deleted = this->Coalesce(node, sib_pg, parent, 
																					sib_index, transaction);
			 		result = false; 
			 }
    }
    else 
    {
       N *sib_pg = (N *)this->buffer_pool_manager_->FetchPage
                                          (parent->ValueAt(rd_sib_idx));
       if(rd_sib_idx < parent_index)
          this->Redistribute(sib_pg, node, 0);
       else 
          this->Redistribute(sib_pg, node, 1);

    	 this->buffer_pool_manager_->UnpinPage(sib_pg->GetPageId(), true);
			 result = false; //Need to Unpin
    }
    
	
		if(!parent_deleted) 
		{		
				this->buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
		}
	
    return result;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *&neighbor_node, N *&node, 
              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
                                            int index, Transaction *transaction)
{
    /*
     *   Neighbor node - Recepient 
     *   Node          - Donor
     *   parent        - parent of donor & recepient
     *   index         - index of donor in parent node
     *   transaction   - not used
     */
    node->MoveAllTo(neighbor_node, index, this->buffer_pool_manager_);

    this->buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    this->buffer_pool_manager_->DeletePage(node->GetPageId());

    parent->Remove(index);

    if(parent->GetSize() < parent->GetMinSize())
		{
        return this->CoalesceOrRedistribute(parent, transaction);
		}		

    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) 
{
    if(index) //Move sibbling page's first to end of input node
		{
        neighbor_node->MoveFirstToEndOf(node, this->buffer_pool_manager_);
		}
    else
		{
        neighbor_node->MoveLastToFrontOf(node, -1, this->buffer_pool_manager_);
		}
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) 
{
		if(old_root_node->GetSize() == 1)
		{
				BPlusTreePage *new_root_pg;
    		this->root_page_id_ = 
               ((B_PLUS_TREE_INTERNAL_PG_PGID *)old_root_node)->ValueAt(0);
				new_root_pg = (BPlusTreePage*)this->buffer_pool_manager_->
																			FetchPage(this->root_page_id_);
				new_root_pg->SetParentPageId(INVALID_PAGE_ID);	
				this->buffer_pool_manager_->UnpinPage(new_root_pg->GetPageId(),
																							true);
		}
		else
		{
				this->root_page_id_ = INVALID_PAGE_ID;
		}
		
	
    this->buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(),true);
    this->buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    this->UpdateRootPageId(false);

    return true; //No need to delete. As the node is already deleted.
}


INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::CheckMergeSibbling(int parent_idx, 
                                       B_PLUS_TREE_INTERNAL_PG_PGID *parent,
                                       int cur_node_size, int node_max_size, 
                                       int &redistribute_idx)
{
    int lnode_size = 0;
    int rnode_size = 0;


    int lidx  = (parent_idx == 0)?(-1):(parent_idx-1);
    int ridx =  (parent_idx == parent->GetSize()-1)?(-1):(parent_idx+1);
   
    if(lidx >= 0)
    {
       BPlusTreePage *bt_pg = 
          (BPlusTreePage *)this->buffer_pool_manager_->FetchPage
                                              (parent->ValueAt(lidx));
       lnode_size = bt_pg->GetSize();
       this->buffer_pool_manager_->UnpinPage(bt_pg->GetPageId(), false);
    }

    if(ridx >= 0)
    { 
       BPlusTreePage *bt_pg = 
          (BPlusTreePage *)this->buffer_pool_manager_->FetchPage
                                            (parent->ValueAt(ridx)); 
       rnode_size = bt_pg->GetSize();
       this->buffer_pool_manager_->UnpinPage(bt_pg->GetPageId(), false);
    }


    if(lidx < 0) 
    {
        redistribute_idx = ridx;
        if(rnode_size+cur_node_size <= node_max_size)
            return ridx;
        return INVALID_INDEX;
    }

    if(ridx < 0)
    {
        redistribute_idx = lidx;
        if(lnode_size+cur_node_size <= node_max_size)
            return lidx;
        return INVALID_INDEX;
    }


    if(lnode_size <= rnode_size)
    {
        redistribute_idx = ridx;
        if(lnode_size+cur_node_size <= node_max_size)
            return lidx;
    }
    else
    {
        redistribute_idx = lidx;
        if(rnode_size+cur_node_size <= node_max_size)
            return ridx;
    }


    /*if(right_child_size == 0 && left_child_size == 0) 
        return INVALID_INDEX;


    if(left_child_size < right_child_size)
    {
        if(left_child_size+node_size <= node_max_size)
            return left_index;
    }
    else
    {
        if(right_child_size+node_size <= node_max_size)
            return right_index;
    }*/

    return INVALID_INDEX;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() 
{ 
    KeyType key;
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_pg = this->FindLeafPage(key, true);
		page_id_t pg_id = leaf_pg->GetPageId();
		this->buffer_pool_manager_->UnpinPage(pg_id, false);
    return INDEXITERATOR_TYPE(this->buffer_pool_manager_, pg_id);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) 
{
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_pg = this->FindLeafPage(key, false);
		page_id_t pg_id = leaf_pg->GetPageId();
		this->buffer_pool_manager_->UnpinPage(pg_id, false);
    return INDEXITERATOR_TYPE(this->buffer_pool_manager_, pg_id);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) 
{   
    BPlusTreePage *page_ptr = 
        (BPlusTreePage*)this->buffer_pool_manager_->FetchPage
                                                      (this->root_page_id_);
    page_id_t pg_id;

    while(!page_ptr->IsLeafPage())
    {
        B_PLUS_TREE_INTERNAL_PG_PGID *int_pg_ptr = 
                      (B_PLUS_TREE_INTERNAL_PG_PGID *)page_ptr;

        if(leftMost)
            pg_id = int_pg_ptr->ValueAt(0);
        else
            pg_id = int_pg_ptr->Lookup(key, this->comparator_);

        this->buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);

        page_ptr = 
            (BPlusTreePage *)this->buffer_pool_manager_->FetchPage(pg_id);
    }
    
    return ((B_PLUS_TREE_LEAF_PAGE_TYPE *)page_ptr);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) 
{
 	 	HeaderPage *header_page = static_cast<HeaderPage *>(
      		buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  	if (insert_record)
    	// create a new record<index_name + root_page_id> in header_page
    	header_page->InsertRecord(index_name_, root_page_id_);
  	else
    	// update root_page_id in header_page
    	header_page->UpdateRecord(index_name_, root_page_id_);
 
	 	buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) 
{	
		BPlusTreePage *pg; 
		std::queue<page_id_t> pg_q;
		std::queue<page_id_t> pin_cnt_q;
		int current_level = 1, next_level = 0;
		
		if(this->root_page_id_ == INVALID_PAGE_ID) return "";


		pg_q.push(this->root_page_id_);

		while(!pg_q.empty())
		{
			page_id_t pg_id = pg_q.front();
			pg = (BPlusTreePage *)this->buffer_pool_manager_->FetchPage(pg_id);
			
			pg_q.pop();

			current_level--;
		
			if(!pg->IsLeafPage())
			{
				for(int x = 0; x < pg->GetSize(); x++)
				{
					pg_q.push(((B_PLUS_TREE_INTERNAL_PG_PGID *)pg)->ValueAt(x));
					next_level++;
				}
			} 
				std::cout<<"[PgId:"<<pg->GetPageId()<<", PC:"<<((Page *)pg)->GetPinCount()<<", MinSz:"<<pg->GetMinSize()<<"]:";

			for(int x=0; x<pg->GetSize(); x++) 
			{
				if(pg->IsLeafPage())
				{
						B_PLUS_TREE_LEAF_PAGE_TYPE *lpg = 
																(B_PLUS_TREE_LEAF_PAGE_TYPE *)pg;
						std::cout<<"["<<x<<"]:"<<lpg->KeyAt(x)<<"\t";
				}
				else
				{
						B_PLUS_TREE_INTERNAL_PG_PGID *ipg = 
																(B_PLUS_TREE_INTERNAL_PG_PGID *)pg;
						if(x!=0)
							std::cout<<"["<<x<<"]:"<<ipg->KeyAt(x)<<"\t";
				}
			}
			std::cout<<std::endl;

			if(current_level == 0)
			{
					std::cout<<"\n\n";
					current_level = next_level;
					next_level = 0;
			}
			
			this->buffer_pool_manager_->UnpinPage(pg->GetPageId(),false);
		}

		return " ";
}


/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb

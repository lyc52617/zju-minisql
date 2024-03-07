#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list.size()==0)
    return false;
  *frame_id = lru_list.back();
  lru_list.erase(--lru_list.end());
  pool.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if(!pool.count(frame_id))
    return;
  lru_list.erase(pool[frame_id]);
  pool.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(pool.count(frame_id))
    return;
  lru_list.push_front(frame_id);
  pool[frame_id] = lru_list.begin();
}

size_t LRUReplacer::Size() {
  return lru_list.size();
}
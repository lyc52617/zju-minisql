#include "page/bitmap_page.h"
#include "glog/logging.h"
template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
	page_offset = next_free_page_;
	uint32_t byte_index = page_offset / 8;
	if(byte_index >= MAX_CHARS){
		return false;
	}
	uint32_t bit_index = page_offset % 8;
	unsigned char mybit = (0x01) << bit_index;
	bytes[byte_index] = bytes[byte_index] | mybit;
	page_allocated_++;
	if(page_allocated_/8 == MAX_CHARS){
		next_free_page_ = page_allocated_;
		return true;
	}
	while(!IsPageFree(next_free_page_)){
		next_free_page_++;
		if(next_free_page_ / 8 >= MAX_CHARS){
			return false;
		}
	}
	return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
		
	uint32_t byte_index = page_offset / 8;
	if(byte_index >= MAX_CHARS){
		return false;
	}
    uint32_t bit_index = page_offset % 8;
	unsigned char mybit = (0x01) << bit_index;
	LOG(INFO)<<(mybit & bytes[byte_index])<<std::endl;
	if((mybit & bytes[byte_index]) != 0){
		;
	}
	else{
		return false;
	}
	mybit = ~mybit;
	bytes[byte_index] = bytes[byte_index] & mybit;

	if(next_free_page_ > page_offset){
		next_free_page_ = page_offset;
	}

	page_allocated_--;
	return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    uint32_t byte_index = page_offset / 8;
	if(byte_index >= MAX_CHARS){
		return false;
	}
    uint32_t bit_index = page_offset % 8;
    return IsPageFreeLow(byte_index, bit_index);
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
    unsigned char mybit = bytes[byte_index];
	int result;
    mybit = mybit >> bit_index;
	result = mybit & 0x01;
	if(result){
		return false;
	}
	else{
		return true;
	}
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;
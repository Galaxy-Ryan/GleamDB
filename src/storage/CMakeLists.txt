set(SOURCES 
        disk_manager.cpp
        disk_scheduler.cpp
        buffer_pool_manager.cpp 
        ../replacer/replacer.h 
        ../replacer/lru_replacer.cpp 
)
add_library(storage STATIC ${SOURCES})

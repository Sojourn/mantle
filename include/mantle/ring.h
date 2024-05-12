#pragma once

#include <vector>
#include <cstdint>
#include <cstddef>
#include "mantle/types.h"

namespace mantle {

    template<typename T>
    class Ring {
    public:
        explicit Ring(size_t minimum_size) {
            size_t size = 1;
            while (size < minimum_size) {
                size = size * 2;
            }

            data_.resize(size);
            mask_ = size - 1;
        }

        size_t size() const {
            return data_.size();
        }

        T& operator[](Sequence sequence) {
            return data_[sequence & mask_];
        }

        const T& operator[](Sequence sequence) const {
            return data_[sequence & mask_];
        }

        void fill(const T& value) {
            for (size_t i = 0; i < data_.size(); ++i) {
                data_[i] = value;
            }
        }

    private:
        std::vector<T> data_;
        size_t         mask_;
    };

}

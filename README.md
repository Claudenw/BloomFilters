# BloomFilter

Simple implementation of Bloom filters and bloom filter driven collections.

## Usage Pattern

* Use a BloomFilterBuilder to digest items and create a ProtoBloomFilter.
* Create a FilterConfig defining the number of items that will be added to the filter and the probability of collisions.
* Use the ProtoBloomFilter to create a BloomFilter with the shape defined by the FilterConfig.

## Notes

* The ProtoBloomFilter can be used to generate multiple BloomFilters using multiple FilterConfigs.
* Creation of the ProtoBloomFilter is far more intensive than the creation of the subsequent BloomFilter.
 

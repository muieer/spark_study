package org.muieer.study.utils;

import org.apache.spark.util.sketch.BloomFilter;

public class BloomFilterUseDemo {

    public BloomFilter create(long num) {

        return BloomFilter.create(num);
    }

    public static void demo() {

        BloomFilter bloomFilter = BloomFilter.create(3);

        bloomFilter.putString("a");
        bloomFilter.putString("b");
        bloomFilter.putString("c");
        bloomFilter.putString("d");

        System.out.println(bloomFilter.mightContainString("e"));
        System.out.println(bloomFilter.mightContainString("a"));

        System.out.println(Math.log(Math.E)); // 底数是自然数e
        System.out.println(1L<<63);

    }

}

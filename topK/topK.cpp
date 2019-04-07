#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>

#include <fstream>
#include <string>

#include "map_reduce.h"

// a set of words
struct passage
{
    char *data;
    uint64_t len;
};

// one word
struct word
{
    char *data;

    // necessary functions to use this as a key
    bool operator<(word const &other) const
    {
        return strcmp(data, other.data) < 0;
    }
    bool operator==(word const &other) const
    {
        return strcmp(data, other.data) == 0;
    }
};

// hash one word
struct hash
{
    // FNV-1a hash for 64 bits
    size_t operator()(word const &key) const
    {
        char *h = key.data;
        uint64_t v = 14695981039346656037ULL;
        while (*h != 0)
            v = (v ^ (size_t)(*(h++))) * 1099511628211ULL;
        return v;
    }
};

class TopK : public MapReduceSort<TopK, passage, word, uint64_t, hash_container<word, uint64_t, sum_combiner, hash>>
{
    char *data;
    uint64_t dataSize;
    uint64_t chunkSize;
    uint64_t splitterPos;

  public:
    explicit TopK(char *_data, uint64_t length, uint64_t _chunkSize) : data(_data), dataSize(length), chunkSize(_chunkSize),
                                                                       splitterPos(0) {}

    void *locate(data_type *str, uint64_t len) const
    {
        return str->data;
    }

    void map(data_type const &input, map_container &out) const
    {
        for (uint64_t i = 0; i < input.len; i++)
        {
            input.data[i] = tolower(input.data[i]);
        }

        uint64_t i = 0;
        while (i < input.len)
        {
            while (i < input.len && (input.data[i] < 'a' || input.data[i] > 'z'))
                i++;
            uint64_t start = i;
            while (i < input.len && ((input.data[i] >= 'a' && input.data[i] <= 'z') || input.data[i] == '\''))
                i++;
            if (i > start)
            {
                input.data[i] = 0;
                word w = {input.data + start};
                emit_intermediate(out, w, 1);
            }
        }
    }

    /** wordcount split()
     *  Memory map the file and divide file on a word border i.e. a space.
     */
    int split(passage &out)
    {
        /* End of data reached, return FALSE. */
        if ((uint64_t)splitterPos >= dataSize)
        {
            return 0;
        }

        /* Determine the nominal end point. */
        uint64_t end = std::min(splitterPos + chunkSize, dataSize);

        /* Move end point to next word break */
        while (end < dataSize &&
               data[end] != ' ' && data[end] != '\t' &&
               data[end] != '\r' && data[end] != '\n')
            end++;

        /* Set the start of the next data. */
        out.data = data + splitterPos;
        out.len = end - splitterPos;

        splitterPos = end;

        /* Return true since the out data is valid. */
        return 1;
    }

    bool sort(keyval const &a, keyval const &b) const
    {
        return a.val < b.val || (a.val == b.val && strcmp(a.key.data, b.key.data) > 0);
    }
};

void getStopWords(std::vector<std::string> &sWords)
{
    std::ifstream file("data/stopWords.txt");
    std::string line;
    while (getline(file, line))
    {
        sWords.push_back(line);
    }
}

int main(int argc, char *argv[])
{
    int fd;
    char *fdata;
    unsigned int dispNums[] = {10, 20};
    struct stat finfo;
    char *fnames[] = {"data/short.txt", "data/medium.txt", "data/long.txt"};
    char *dispNum_str;
    struct timeval start, end;

    std::vector<std::string> stopWords;
    getStopWords(stopWords);

    for (unsigned int i = 0; i < 3; ++i)
    {
        char *fname = fnames[i];

        // Read in the file
        CHECK_ERROR((fd = open(fname, O_RDONLY)) < 0);
        // Get the file info (for file length)
        CHECK_ERROR(fstat(fd, &finfo) < 0);

        uint64_t k = 0;

        fdata = (char *)malloc(finfo.st_size);
        CHECK_ERROR(fdata == NULL);
        while (k < (uint64_t)finfo.st_size)
        {
            k += pread(fd, fdata + k, finfo.st_size, k);
        }

        CHECK_ERROR(k != (uint64_t)finfo.st_size);

        for (unsigned int j = 0; j < 2; ++j)
        {
            unsigned int dispNum = dispNums[j];

            printf("\nFinding the top %d most common words in %s \n", dispNum, fname);

            std::vector<TopK::keyval> output;

            gettimeofday(&start, NULL);
            TopK mapReduce(fdata, finfo.st_size, 1024 * 1024);
            CHECK_ERROR(mapReduce.run(output) < 0);
            gettimeofday(&end, NULL);

            float runTime = (end.tv_sec - start.tv_sec) * 1000000;
            runTime += (end.tv_usec - start.tv_usec);

            unsigned int num = std::min(dispNum, (unsigned int)output.size());
            printf("\nOut of %lu words, the top %d are:\n", output.size(), num);
            uint64_t total = 0;
            for (size_t i = 0; i < num; i++)
            {
                std::string str = output[output.size() - 1 - i].key.data;
                if (std::find(stopWords.begin(), stopWords.end(), str) == stopWords.end())
                {
                    printf("%15s - %lu\n", output[output.size() - 1 - i].key.data, output[output.size() - 1 - i].val);
                }
                else
                {
                    // omit the result if it is a stop word
                    if (num + 1 < output.size())
                    {
                        num++;
                    }
                }
            }

            printf("\nThe time taken to find the top %d words in %s took %f ms\n\n\n", dispNum, fname, (runTime / 1000L));
        }

        free(fdata);

        CHECK_ERROR(close(fd) < 0);
    }

    return 0;
}
#include <unistd.h>
#include "ParallelBB.h"

template <typename Key, typename Value>
class MinimalHashmap
{
public:
	MinimalHashmap() {}
	MinimalHashmap(const std::vector<Key>& inkeys, const std::vector<Value>& invalues, size_t numThreads)
	{
		build(inkeys, invalues, numThreads);
	}
	void build(const std::vector<Key>& inkeys, const std::vector<Value>& invalues, size_t numThreads)
	{
		assert(inkeys.size() == invalues.size());
		locator.build(numThreads, inkeys);
		keys.resize(inkeys.size());
		values.resize(inkeys.size());
		std::vector<std::thread> splitterThreads;
		splitterThreads.reserve(numThreads);
		for (size_t thread = 0; thread < numThreads; thread++)
		{
			splitterThreads.emplace_back([this, &inkeys, &invalues, thread, numThreads](){
				for (size_t i = thread; i < inkeys.size(); i += numThreads)
				{
					auto pos = locator.lookup(inkeys[i]);
					keys[pos] = inkeys[i];
					values[pos] = invalues[i];
				}
			});
		}
		for (size_t thread = 0; thread < numThreads; thread++)
		{
			splitterThreads[thread].join();
		}
	}
	Value& operator[](const Key& key)
	{
		auto pos = find(key);
		assert(pos != keys.size());
		return values[pos];
	}
	const Value& operator[](const Key& key) const
	{
		auto pos = find(key);
		assert(pos != keys.size());
		return values[pos];
	}
	bool contains(const Key& key) const
	{
		return find(key) != keys.size();
	}
	size_t size() const
	{
		return keys.size();
	}
private:
	size_t find(const Key& key) const
	{
		auto pos = locator.lookup(key);
		if (pos == -1) return keys.size();
		if (keys[pos] != key) return keys.size();
		return pos;
	}
	ParallelBB<Key> locator;
	std::vector<Key> keys;
	std::vector<Value> values;
};
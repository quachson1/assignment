package com.quachson.rest.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class MFUCache<Key, Value> implements Map<Key, Value> {

	private final Map<Key, CacheNode<Key, Value>> cache;
	private final LinkedHashSet[] frequencyList;
	private int hightestFrequency;
	private int maxFrequency;
	//
	private final int maxCacheSize;
	private final float evictionFactor;

	public MFUCache(int maxCacheSize, float evictionFactor) {
		this.cache = new HashMap<Key, CacheNode<Key, Value>>(maxCacheSize);
		this.frequencyList = new LinkedHashSet[maxCacheSize];
		this.hightestFrequency = 0;
		this.maxFrequency = maxCacheSize - 1;
		this.maxCacheSize = maxCacheSize;
		this.evictionFactor = evictionFactor;
		initFrequencyList();
	}

	public Value put(Key k, Value v) {
		Value oldValue = null;
		CacheNode<Key, Value> currentNode = cache.get(k);
		if (currentNode == null) {
			if (cache.size() == maxCacheSize) {
				doEviction();
			}
			LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[0];
			currentNode = new CacheNode(k, v, 0);
			nodes.add(currentNode);
			cache.put(k, currentNode);
		} else {
			oldValue = currentNode.v;
			currentNode.v = v;
		}
		return oldValue;
	}

	public void putAll(Map<? extends Key, ? extends Value> map) {
		for (Map.Entry<? extends Key, ? extends Value> me : map.entrySet()) {
			put(me.getKey(), me.getValue());
		}
	}

	public Value get(Object k) {
		CacheNode<Key, Value> currentNode = cache.get(k);
		if (currentNode != null) {
			int currentFrequency = currentNode.frequency;
			if (currentFrequency < maxFrequency) {
				int nextFrequency = currentFrequency + 1;
				LinkedHashSet<CacheNode<Key, Value>> currentNodes = frequencyList[currentFrequency];
				LinkedHashSet<CacheNode<Key, Value>> newNodes = frequencyList[nextFrequency];
				moveToNextFrequency(currentNode, nextFrequency, currentNodes, newNodes);
				cache.put((Key) k, currentNode);
				
				if (hightestFrequency < nextFrequency) {
					hightestFrequency = nextFrequency;
				}
			} else {
				// Hybrid with MRU: put most recently accessed ahead of others:
				LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[currentFrequency];
				nodes.remove(currentNode);
				nodes.add(currentNode);
			}
			return currentNode.v;
		} else {
			return null;
		}
	}

	public Value remove(Object k) {
		CacheNode<Key, Value> currentNode = cache.remove(k);
		if (currentNode != null) {
			LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[currentNode.frequency];
			nodes.remove(currentNode);
			if (hightestFrequency == currentNode.frequency) {
				findNextMostFrequency();
			}
			return currentNode.v;
		} else {
			return null;
		}
	}

	public int frequencyOf(Key k) {
		CacheNode<Key, Value> node = cache.get(k);
		if (node != null) {
			return node.frequency + 1;
		} else {
			return 0;
		}
	}

	public void clear() {
		for (int i = 0; i <= maxFrequency; i++) {
			frequencyList[i].clear();
		}
		cache.clear();
		hightestFrequency = 0;
	}

	public Set<Key> keySet() {
		return this.cache.keySet();
	}

	public Collection<Value> values() {
		return null; // To change body of implemented methods use File |
						// Settings | File Templates.
	}

	public Set<Entry<Key, Value>> entrySet() {
		return null; // To change body of implemented methods use File |
						// Settings | File Templates.
	}

	public int size() {
		return cache.size();
	}

	public boolean isEmpty() {
		return this.cache.isEmpty();
	}

	public boolean containsKey(Object o) {
		return this.cache.containsKey(o);
	}

	public boolean containsValue(Object o) {
		return false; // To change body of implemented methods use File |
						// Settings | File Templates.
	}

	private void initFrequencyList() {
		for (int i = 0; i <= maxFrequency; i++) {
			frequencyList[i] = new LinkedHashSet<CacheNode<Key, Value>>();
		}
	}

	private void doEviction() {
		int currentlyDeleted = 0;
		float target = maxCacheSize * evictionFactor;
		while (currentlyDeleted < target) {
			LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[hightestFrequency];
			if (nodes.isEmpty()) {
				throw new IllegalStateException("The most frequency constraint violated!");
			} else {
				Iterator<CacheNode<Key, Value>> it = nodes.iterator();
				while (it.hasNext() && currentlyDeleted++ < target) {
					CacheNode<Key, Value> node = it.next();
					it.remove();
					cache.remove(node.k);
				}
				if (!it.hasNext()) {
					findNextMostFrequency();
				}
			}
		}
	}

	private void moveToNextFrequency(CacheNode<Key, Value> currentNode, int nextFrequency,
			LinkedHashSet<CacheNode<Key, Value>> currentNodes, LinkedHashSet<CacheNode<Key, Value>> newNodes) {
		currentNodes.remove(currentNode);
		newNodes.add(currentNode);
		currentNode.frequency = nextFrequency;
	}

	private void findNextMostFrequency() {
		while (hightestFrequency <= maxFrequency && frequencyList[hightestFrequency].isEmpty()) {
			--hightestFrequency;
		}
		if (hightestFrequency < 0) {
			hightestFrequency = 0;
		}
	}

	public String printKey() {
		final StringBuilder sb = new StringBuilder("");

		for (java.util.Map.Entry<Key, CacheNode<Key, Value>> entry : cache.entrySet()) {
			sb.append(entry.getKey()).append(", ");
		}
		return sb.toString();
	}

	@Override
	public String toString() {
		return "MFUCache [cache=" + cache + ", frequencyList=" + Arrays.toString(frequencyList) + ", hightestFrequency="
				+ hightestFrequency + ", maxFrequency=" + maxFrequency + ", maxCacheSize=" + maxCacheSize + "]";
	}

	private static class CacheNode<Key, Value> {

		public final Key k;
		public Value v;
		public int frequency;

		public CacheNode(Key k, Value v, int frequency) {
			this.k = k;
			this.v = v;
			this.frequency = frequency;
		}

		@Override
		public String toString() {
			return "CacheNode [k=" + k + ", v=" + v + ", frequency=" + frequency + "]";
		}

	}
}

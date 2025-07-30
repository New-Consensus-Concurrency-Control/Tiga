#pragma once
#include <cstdint>
#include <iostream>
#include <queue>
#include <set>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct SCCContext {
   const std::unordered_map<uint64_t, std::unordered_set<uint64_t>>* graph;
   std::unordered_map<uint64_t, int> indices;
   std::unordered_map<uint64_t, int> lowLinks;
   std::stack<uint64_t> nodeStack;
   std::unordered_set<uint64_t> onStack;
   std::vector<std::set<uint64_t>>* sccs;
   int index;
};

void tarjanDFS(uint64_t node, SCCContext* ctx);
std::vector<std::set<uint64_t>>* findSCCs(
    const std::unordered_map<uint64_t, std::unordered_set<uint64_t>>& graph);

bool isSCCConnected(
    const std::set<uint64_t>& scc1, const std::set<uint64_t>& scc2,
    const std::unordered_map<uint64_t, std::unordered_set<uint64_t>>& graph);

std::vector<uint32_t> topologicalSort(
    const std::unordered_map<uint32_t, std::unordered_set<uint32_t>>&
        condensationGraph);
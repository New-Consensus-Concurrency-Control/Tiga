#include "SCCFinder.h"

void tarjanDFS(uint64_t node, SCCContext* ctx) {
   ctx->indices[node] = ctx->lowLinks[node] = ctx->index++;
   ctx->nodeStack.push(node);
   ctx->onStack.insert(node);

   for (uint64_t neighbor : ctx->graph->at(node)) {
      if (ctx->indices.find(neighbor) == ctx->indices.end()) {
         tarjanDFS(neighbor, ctx);
         ctx->lowLinks[node] =
             std::min(ctx->lowLinks[node], ctx->lowLinks[neighbor]);
      } else if (ctx->onStack.find(neighbor) != ctx->onStack.end()) {
         ctx->lowLinks[node] =
             std::min(ctx->lowLinks[node], ctx->indices[neighbor]);
      }
   }

   if (ctx->lowLinks[node] == ctx->indices[node]) {
      std::set<uint64_t> scc;
      uint64_t w;
      do {
         w = ctx->nodeStack.top();
         ctx->nodeStack.pop();
         ctx->onStack.erase(w);
         scc.insert(w);
      } while (w != node);
      ctx->sccs->push_back(scc);
   }
}

std::vector<std::set<uint64_t>>* findSCCs(
    const std::unordered_map<uint64_t, std::unordered_set<uint64_t>>& graph) {
   SCCContext ctx;
   ctx.graph = &graph;
   ctx.index = 0;
   ctx.sccs = new std::vector<std::set<uint64_t>>();

   for (const auto& [node, _] : graph) {
      if (ctx.indices.find(node) == ctx.indices.end()) {
         tarjanDFS(node, &ctx);
      }
   }
   return ctx.sccs;
}

bool isSCCConnected(
    const std::set<uint64_t>& scc1, const std::set<uint64_t>& scc2,
    const std::unordered_map<uint64_t, std::unordered_set<uint64_t>>& graph) {
   for (const auto& node1 : scc1) {
      for (const auto& node2 : scc2) {
         const auto iter1 = graph.find(node1);
         if (iter1 != graph.end() &&
             iter1->second.find(node2) != iter1->second.end()) {
            return true;
         }
      }
   }
   return false;
}

std::vector<uint32_t> topologicalSort(
    const std::unordered_map<uint32_t, std::unordered_set<uint32_t>>&
        condensationGraph) {
   std::unordered_map<uint32_t, int> inDegree;
   std::queue<uint32_t> zeroInDegree;
   std::vector<uint32_t> sortedOrder;

   for (const auto& [node, neighbors] : condensationGraph) {
      if (inDegree.find(node) == inDegree.end()) {
         inDegree[node] = 0;
      }
      for (uint32_t neighbor : neighbors) {
         inDegree[neighbor]++;
      }
   }

   for (const auto& [node, degree] : inDegree) {
      if (degree == 0) {
         zeroInDegree.push(node);
      }
   }

   while (!zeroInDegree.empty()) {
      uint32_t node = zeroInDegree.front();
      zeroInDegree.pop();
      sortedOrder.push_back(node);

      for (uint32_t neighbor : condensationGraph.at(node)) {
         if (--inDegree[neighbor] == 0) {
            zeroInDegree.push(neighbor);
         }
      }
   }

   return sortedOrder;
}

// int main() {
//    std::unordered_map<uint64_t, std::unordered_set<uint64_t>> graph = {
//        {0, {1}}, {1, {2}}, {2, {0, 3}}, {3, {2, 4}}, {4, {5, 6}},
//        {5, {3}}, {6, {7}}, {7, {8}},    {8, {6}}};

//    std::vector<std::set<uint64_t>> sccs = findSCCs(graph);

//    std::cout << "Strongly Connected Components:" << std::endl;
//    for (const auto& scc : sccs) {
//       for (uint64_t node : scc) {
//          std::cout << node << " ";
//       }
//       std::cout << std::endl;
//    }
//    return 0;
// }

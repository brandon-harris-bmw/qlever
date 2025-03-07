// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H

#include <utility>

#include "engine/TransitivePathBase.h"
#include "util/Timer.h"

namespace detail {

// Helper struct that allows to group a read-only view of a column of a table
// with a reference to the table itself and a local vocabulary (used to ensure
// the correct lifetime).
template <typename ColumnType>
struct TableColumnWithVocab {
  const IdTable* table_;
  ColumnType column_;
  LocalVocab vocab_;

  // Explicit to prevent issues with co_yield and lifetime.
  // See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103909 for more info.
  TableColumnWithVocab(const IdTable* table, ColumnType column,
                       LocalVocab vocab)
      : table_{table}, column_{std::move(column)}, vocab_{std::move(vocab)} {}
};
};  // namespace detail

/**
 * @class TransitivePathImpl
 * @brief This class implements common functions for the concrete TransitivePath
 * classes TransitivePathBinSearch and TransitivePathHashMap. The template can
 * be set to a map data structure which is used for the transitive hull
 * computation.
 *
 * @tparam T A map data structure for the transitive hull computation.
 */
template <typename T>
class TransitivePathImpl : public TransitivePathBase {
  using TableColumnWithVocab =
      detail::TableColumnWithVocab<std::span<const Id>>;

 public:
  using TransitivePathBase::TransitivePathBase;

  /**
   * @brief Compute the transitive hull with a bound side.
   * This generator is used when the startSide is bound and
   * it is a variable. The other IdTable contains the result
   * of the start side and will be used to get the start nodes.
   */
  struct ComputeTransitivePathBound
      : ad_utility::InputRangeFromGet<Result::IdTableVocabPair> {
    const TransitivePathImpl& parent_;
    ad_utility::Timer timer_;
    // This field is a pointer instead of a value so that references to the data
    // will remain valid after moving the generator
    std::unique_ptr<T> edges_;
    Result::Generator result_;
    std::optional<Result::Generator::iterator> nextResult_;

    /**
     * @param parent A reference to the TransitivePathImpl which uses this
     * generator
     * @param sub A shared pointer to the sub result. Needs to be kept alive for
     * the lifetime of this generator.
     * @param startSide The start side for the transitive hull
     * @param targetSide The target side for the transitive hull
     * @param startSideResult The Result of the startSide
     * @param yieldOnce If true, the generator will yield only a single time.
     */
    ComputeTransitivePathBound(const TransitivePathImpl& parent,
                               std::shared_ptr<const Result> sub,
                               const TransitivePathSide& startSide,
                               const TransitivePathSide& targetSide,
                               std::shared_ptr<const Result> startSideResult,
                               bool yieldOnce)
        : parent_{parent},
          timer_{ad_utility::Timer::Started},
          edges_{std::make_unique<T>(
              parent_.setupEdgesMap(sub->idTable(), startSide, targetSide))} {
      auto nodes = SetupNodes{startSide, std::move(startSideResult)};
      // Setup nodes returns a generator, so this time measurement won't include
      // the time for each iteration, but every iteration step should have
      // constant overhead, which should be safe to ignore.
      parent_.runtimeInfo().addDetail("Initialization time",
                                      timer_.msecs().count());

      std::unique_ptr<NodeGenerator> hull = 
          // parent_.transitiveHull(
          std::make_unique<TransitiveHull<SetupNodes, std::span<const Id>>>(parent_,
          *edges_, sub->getCopyOfLocalVocab(), std::move(nodes),
          targetSide.isVariable()
              ? std::nullopt
              : std::optional{std::get<Id>(targetSide.value_)},
          yieldOnce);

      result_ = parent_.fillTableWithHull(
          std::move(hull), startSide.outputCol_, targetSide.outputCol_,
          startSide.treeAndCol_.value().second, yieldOnce,
          startSide.treeAndCol_.value().first->getResultWidth());
    }

    std::optional<Result::IdTableVocabPair> get() {
      if (!nextResult_.has_value()) {
        nextResult_ = result_.begin();
      } else if (nextResult_.value() != result_.end()) {
        nextResult_.value()++;
      }
      if (nextResult_.value() == result_.end()) {
        return std::nullopt;
      }
      return std::optional{std::move(*(nextResult_.value()))};
    }
  };

  /**
   * @brief Compute the transitive hull.
   * This generator is used when no side is bound (or an id).
   */
  struct ComputeTransitivePath
      : ad_utility::InputRangeFromGet<Result::IdTableVocabPair> {
    const TransitivePathImpl& parent_;
    ad_utility::Timer timer_;
    // These fields are pointers instead of values so that references to the
    // data will remain valid after moving the generator
    std::unique_ptr<T> edges_;
    std::unique_ptr<Set> nodesWithoutDuplicates_;
    std::unique_ptr<detail::TableColumnWithVocab<const Set&>> tableInfo_;
    Result::Generator result_;
    std::optional<Result::Generator::iterator> nextResult_;

    /**
     * @param parent A reference to the TransitivePathImpl which uses this
     * generator
     * @param sub A shared pointer to the sub result. Needs to be kept alive for
     * the lifetime of this generator.
     * @param startSide The start side for the transitive hull
     * @param targetSide The target side for the transitive hull
     * @param yieldOnce If true, the generator will yield only a single time.
     */
    ComputeTransitivePath(const TransitivePathImpl& parent,
                          std::shared_ptr<const Result> sub,
                          const TransitivePathSide& startSide,
                          const TransitivePathSide& targetSide, bool yieldOnce)
        : parent_{parent},
          timer_{ad_utility::Timer::Started},
          edges_{std::make_unique<T>(
              parent_.setupEdgesMap(sub->idTable(), startSide, targetSide))},
          nodesWithoutDuplicates_{std::make_unique<Set>(parent_.allocator())} {
      auto nodesWithDuplicates =
          parent_.setupNodes(sub->idTable(), startSide, targetSide);
      for (const auto& span : nodesWithDuplicates) {
        nodesWithoutDuplicates_->insert(span.begin(), span.end());
      }

      parent_.runtimeInfo().addDetail("Initialization time", timer_.msecs());

      // Technically we should pass the localVocab of `sub` here, but this will
      // just lead to a merge with itself later on in the pipeline.
      tableInfo_ = std::make_unique<detail::TableColumnWithVocab<const Set&>>(
          nullptr, *nodesWithoutDuplicates_, LocalVocab{});

      std::unique_ptr<NodeGenerator> hull = 
          // parent_.transitiveHull(
          std::make_unique<TransitiveHull<std::span<detail::TableColumnWithVocab<const Set&>>, const Set>>(parent_,
          *edges_, sub->getCopyOfLocalVocab(), std::span{tableInfo_.get(), 1},
          targetSide.isVariable()
              ? std::nullopt
              : std::optional{std::get<Id>(targetSide.value_)},
          yieldOnce);

      result_ = parent_.fillTableWithHull(std::move(hull), startSide.outputCol_,
                                          targetSide.outputCol_, yieldOnce);
    }

    std::optional<Result::IdTableVocabPair> get() {
      if (!nextResult_.has_value()) {
        nextResult_ = result_.begin();
      } else if (nextResult_.value() != result_.end()) {
        nextResult_.value()++;
      }
      if (nextResult_.value() == result_.end()) {
        return std::nullopt;
      }
      return std::optional{std::move(*(nextResult_.value()))};
    }
  };

  protected :
      /**
       * @brief Compute the result for this TransitivePath operation
       * This function chooses the start and target side for the transitive
       * hull computation. This choice of the start side has a large impact
       * on the time it takes to compute the hull. The set of nodes on the
       * start side should be as small as possible.
       *
       * @return Result The result of the TransitivePath operation
       */
      Result
      computeResult(bool requestLaziness) override {
    auto [startSide, targetSide] = decideDirection();
    // In order to traverse the graph represented by this result, we need random
    // access across the whole table, so it doesn't make sense to lazily compute
    // the result.
    std::shared_ptr<const Result> subRes = subtree_->getResult(false);

    if (startSide.isBoundVariable()) {
      std::shared_ptr<const Result> sideRes =
          startSide.treeAndCol_.value().first->getResult(true);

      auto gen = ComputeTransitivePathBound{
          *this,      std::move(subRes),  startSide,
          targetSide, std::move(sideRes), !requestLaziness};

      return requestLaziness
                 ? Result{Result::LazyResult{std::move(gen)}, resultSortedOn()}
                 : Result{cppcoro::getSingleElement<Result::IdTableVocabPair>(
                              Result::LazyResult{std::move(gen)}),
                          resultSortedOn()};
    }
    auto gen = ComputeTransitivePath{*this, std::move(subRes), startSide,
                                     targetSide, !requestLaziness};
    return requestLaziness
               ? Result{Result::LazyResult{std::move(gen)}, resultSortedOn()}
               : Result{cppcoro::getSingleElement<Result::IdTableVocabPair>(
                            Result::LazyResult{std::move(gen)}),
                        resultSortedOn()};
  }

  /**
   * @brief Depth-first search to find connected nodes in the graph.
   * @param edges The adjacency lists, mapping Ids (nodes) to their connected
   * Ids.
   * @param startNode The node to start the search from.
   * @param target Optional target Id. If supplied, only paths which end in this
   * Id are added to the result.
   * @return A set of connected nodes in the graph.
   */
  Set findConnectedNodes(const T& edges, Id startNode,
                         const std::optional<Id>& target) const {
    std::vector<std::pair<Id, size_t>> stack;
    ad_utility::HashSetWithMemoryLimit<Id> marks{
        getExecutionContext()->getAllocator()};
    Set connectedNodes{getExecutionContext()->getAllocator()};
    stack.emplace_back(startNode, 0);

    while (!stack.empty()) {
      checkCancellation();
      auto [node, steps] = stack.back();
      stack.pop_back();

      if (steps <= maxDist_ && !marks.contains(node)) {
        if (steps >= minDist_) {
          marks.insert(node);
          if (!target.has_value() || node == target.value()) {
            connectedNodes.insert(node);
          }
        }

        const auto& successors = edges.successors(node);
        for (auto successor : successors) {
          stack.emplace_back(successor, steps + 1);
        }
      }
    }
    return connectedNodes;
  }

  /**
   * @brief Compute the transitive hull starting at the given nodes,
   * using the given Map.
   */
  template<typename Node, typename NodeColumnType>
  struct TransitiveHull
   : ad_utility::InputRangeFromGet<NodeWithTargets> {
    const TransitivePathImpl& parent_;
    ad_utility::Timer timer_;
    const T& edges_;
    LocalVocab edgesVocab_;
    Node startNodes_;
    std::optional<Id> target_;
    bool yieldOnce_;
    std::optional<typename Node::iterator> nextColumn_;
    std::optional<typename NodeColumnType::const_iterator> nextNode_;
    LocalVocab mergedVocab_;
    size_t currentRow_;
    bool returnedValueLastGetCall_ = false;

    /**
     * @param parent A reference to the TransitivePathImpl which uses this
     * generator
     * @param edges Adjacency lists, mapping Ids (nodes) to their connected
     * Ids.
     * @param edgesVocab The `LocalVocab` holding the vocabulary of the edges.
     * @param startNodes A range that yields an instantiation of
     * `TableColumnWithVocab` that can be consumed to create a transitive hull.
     * @param target Optional target Id. If supplied, only paths which end
     * in this Id are added to the hull.
     * @param yieldOnce This has to be set to the same value as the consuming
     * code. When set to true, this will prevent yielding the same LocalVocab over
     * and over again to make merging faster (because merging with an empty
     * LocalVocab is a no-op).
     * @return Map Maps each Id to its connected Ids in the transitive hull
     */
     TransitiveHull(const TransitivePathImpl& parent, const T& edges, LocalVocab edgesVocab, Node startNodes,
                    std::optional<Id> target, bool yieldOnce)
        : parent_{parent},
          timer_{ad_utility::Timer::Stopped},
          edges_{edges},
          edgesVocab_{std::move(edgesVocab)},
          startNodes_{std::move(startNodes)},
          target_{std::move(target)},
          yieldOnce_{yieldOnce} {}

    std::optional<NodeWithTargets> get() {
      if (!nextColumn_.has_value()) {
        nextColumn_ = startNodes_.begin();
      }
      while (nextColumn_.value() != startNodes_.end()) {
        auto&& tableColumn = *(nextColumn_.value());
        
        // If we returned a value on the last call, skip setup steps.
        // Rerun setup after the column is completely iterated. Iterator will
        // be reset at the end of column iteration
        if (!nextNode_.has_value()) {
          timer_.cont();
          mergedVocab_ = std::move(tableColumn.vocab_);
          mergedVocab_.mergeWith(std::span{&edgesVocab_, 1});
          currentRow_ = 0;
          nextNode_ = std::optional(tableColumn.column_.begin());
        }
        while (nextNode_.value() != tableColumn.column_.end()) {
          if (!returnedValueLastGetCall_) {
            Id startNodeId = *(nextNode_.value());
            Set connectedNodes = parent_.findConnectedNodes(edges_, startNodeId, target_);
            if (!connectedNodes.empty()) {
              returnedValueLastGetCall_ = true;
              parent_.runtimeInfo().addDetail("Hull time", timer_.msecs());
              timer_.stop();
              return NodeWithTargets{startNodeId, std::move(connectedNodes),
                                     mergedVocab_.clone(), tableColumn.table_,
                                     currentRow_};
            }
          }
          if (returnedValueLastGetCall_) {
            timer_.cont();
            // Reset vocab to prevent merging the same vocab over and over again.
            if (yieldOnce_) {
              mergedVocab_ = LocalVocab{};
            }
            returnedValueLastGetCall_ = false;
          }
          currentRow_++;
          nextNode_.value()++;
        }
        // Reset iterator so it can be reused for the next inner loop during
        // the execution of the next iteration of the outer loop
        nextNode_.reset();
        nextColumn_.value()++;
        timer_.stop();
      }
      // Don't reset nextColumn_ iterator. We have exhausted the values that 
      // this generator should provide. Subsequent calls should only return
      // nullopt
      return std::nullopt;
    }
  };

  // CPP_template(typename Node)(requires ql::ranges::range<Node>) NodeGenerator
  //     transitiveHull(const T& edges, LocalVocab edgesVocab, Node startNodes,
  //                    std::optional<Id> target, bool yieldOnce) const {
  //   ad_utility::Timer timer{ad_utility::Timer::Stopped};
  //   for (auto&& tableColumn : startNodes) {
  //     timer.cont();
  //     LocalVocab mergedVocab = std::move(tableColumn.vocab_);
  //     mergedVocab.mergeWith(edgesVocab);
  //     size_t currentRow = 0;
  //     for (Id startNode : tableColumn.column_) {
  //       Set connectedNodes = findConnectedNodes(edges, startNode, target);
  //       if (!connectedNodes.empty()) {
  //         runtimeInfo().addDetail("Hull time", timer.msecs());
  //         timer.stop();
  //         co_yield NodeWithTargets{startNode, std::move(connectedNodes),
  //                                  mergedVocab.clone(), tableColumn.table_,
  //                                  currentRow};
  //         timer.cont();
  //         // Reset vocab to prevent merging the same vocab over and over again.
  //         if (yieldOnce) {
  //           mergedVocab = LocalVocab{};
  //         }
  //       }
  //       currentRow++;
  //     }
  //     timer.stop();
  //   }
  // }

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @param sub The sub table result
   * @param startSide The TransitivePathSide where the edges start
   * @param targetSide The TransitivePathSide where the edges end
   * @return std::vector<std::span<const Id>> An vector of spans of (nodes) for
   * the transitive hull computation
   */
  std::vector<std::span<const Id>> setupNodes(
      const IdTable& sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide) const {
    std::vector<std::span<const Id>> result;

    // id -> var|id
    if (!startSide.isVariable()) {
      result.emplace_back(&std::get<Id>(startSide.value_), 1);
      // var -> var
    } else {
      std::span<const Id> startNodes = sub.getColumn(startSide.subCol_);
      result.emplace_back(startNodes);
      if (minDist_ == 0) {
        std::span<const Id> targetNodes = sub.getColumn(targetSide.subCol_);
        result.emplace_back(targetNodes);
      }
    }

    return result;
  }

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   */
  struct SetupNodes : ad_utility::InputRangeFromGet<TableColumnWithVocab> {
    const TransitivePathSide& startSide_;
    std::shared_ptr<const Result> startSideResult_;
    bool fullyMaterializedResult_;
    bool fullyMaterializedResultReturned_{false};
    std::optional<Result::LazyResult> startSideResultIdTables_;
    std::optional<Result::LazyResult::iterator> startSideResultNext_;

    /**
     * @param startSide The TransitivePathSide where the edges start
     * @param startSideResult A `Result` wrapping an `IdTable` containing the
     * Ids for the startSide
     */
    SetupNodes(const TransitivePathSide& startSide,
               std::shared_ptr<const Result> startSideResult)
        : startSide_{startSide},
          startSideResult_{std::move(startSideResult)},
          fullyMaterializedResult_{startSideResult_->isFullyMaterialized()} {
      if (!fullyMaterializedResult_) {
        startSideResultIdTables_ = std::optional(
            Result::LazyResult{std::move(startSideResult_->idTables())});
      }
    }

    std::optional<TableColumnWithVocab> get() {
      if (fullyMaterializedResult_) {
        if (fullyMaterializedResultReturned_) {
          return std::nullopt;
        } else {
          std::span<const Id> startNodes =
              startSideResult_->idTable().getColumn(
                  startSide_.treeAndCol_.value().second);
          fullyMaterializedResultReturned_ = true;
          return TableColumnWithVocab{&startSideResult_->idTable(), startNodes,
                                      startSideResult_->getCopyOfLocalVocab()};
        }
      } else {
        AD_CONTRACT_CHECK(startSideResultIdTables_.has_value());
        if (!startSideResultNext_.has_value()) {
          startSideResultNext_ = startSideResultIdTables_.value().begin();
        } else if (startSideResultNext_.value() !=
                   startSideResultIdTables_.value().end()) {
          startSideResultNext_.value()++;
        }
        if (startSideResultNext_.value() ==
            startSideResultIdTables_.value().end()) {
          return std::nullopt;
        }
        auto& idTable = startSideResultNext_.value()->idTable_;
        auto& localVocab = startSideResultNext_.value()->localVocab_;
        std::span<const Id> startNodes =
            idTable.getColumn(startSide_.treeAndCol_.value().second);
        return TableColumnWithVocab{&idTable, startNodes,
                                    std::move(localVocab)};
      }
    }
  };

  virtual T setupEdgesMap(const IdTable& dynSub,
                          const TransitivePathSide& startSide,
                          const TransitivePathSide& targetSide) const = 0;
};

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H

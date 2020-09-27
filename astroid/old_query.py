# -*- coding: utf-8 -*-

"""this module contains a set of functions to handle query on astroid trees
"""

import functools
import itertools
import operator

import wrapt
from astroid import bases
from astroid import context as contextmod
from astroid import exceptions
from astroid import decorators
from astroid import helpers
from astroid import manager
from astroid import nodes
from astroid.interpreter import dunder_lookup
from astroid import protocols
from astroid import util


MANAGER = manager.AstroidManager()
# Prevents circular imports
objects = util.lazy_import("objects")


# .query method ###############################################################


def query_end(self, context=None):
    """Inference's end for nodes that yield themselves on Inference

    These are objects for which Inference does not have any semantic,
    such as Module or Consts.
    """
    yield self

# @decorators.raise_if_nothing_queried
# @decorators.path_wrapper
def query_lambda(self, context=None):
    return self.body.query(context)


nodes.Module._query = query_end
nodes.ClassDef._query = query_end
nodes.Lambda._query = query_lambda
nodes.Const._query = query_end
nodes.Slice._query = query_end


def _query_sequence_helper(node, context=None):
    """query all values based on _BaseContainer.elts"""
    values = []

    for elt in node.elts:
        if isinstance(elt, nodes.Starred):
            starred = helpers.safe_query(elt.value, context)
            if not starred:
                raise exceptions.InferenceError(node=node, context=context)
            if not hasattr(starred, "elts"):
                raise exceptions.InferenceError(node=node, context=context)
            values.extend(_query_sequence_helper(starred))
        elif isinstance(elt, nodes.NamedExpr):
            value = helpers.safe_query(elt.value, context)
            if not value:
                raise exceptions.InferenceError(node=node, context=context)
            values.append(value)
        else:
            values.append(elt)
    return values


@decorators.raise_if_nothing_queried
def query_sequence(self, context=None):
    has_starred_named_expr = any(
        isinstance(e, (nodes.Starred, nodes.NamedExpr)) for e in self.elts
    )
    if has_starred_named_expr:
        values = _query_sequence_helper(self, context)
        new_seq = type(self)(
            lineno=self.lineno, col_offset=self.col_offset, parent=self.parent
        )
        new_seq.postinit(values)

        yield new_seq
    else:
        yield self


nodes.List._query = query_sequence
nodes.Tuple._query = query_sequence
nodes.Set._query = query_sequence


def query_map(self, context=None):
    if not any(isinstance(k, nodes.DictUnpack) for k, _ in self.items):
        yield self
    else:
        items = _query_map(self, context)
        new_seq = type(self)(self.lineno, self.col_offset, self.parent)
        new_seq.postinit(list(items.items()))
        yield new_seq


def _update_with_replacement(lhs_dict, rhs_dict):
    """Delete nodes that equate to duplicate keys

    Since an astroid node doesn't 'equal' another node with the same value,
    this function uses the as_string method to make sure duplicate keys
    don't get through

    Note that both the key and the value are astroid nodes

    Fixes issue with DictUnpack causing duplicte keys
    in queried Dict items

    :param dict(nodes.NodeNG, nodes.NodeNG) lhs_dict: Dictionary to 'merge' nodes into
    :param dict(nodes.NodeNG, nodes.NodeNG) rhs_dict: Dictionary with nodes to pull from
    :return dict(nodes.NodeNG, nodes.NodeNG): merged dictionary of nodes
    """
    combined_dict = itertools.chain(lhs_dict.items(), rhs_dict.items())
    # Overwrite keys which have the same string values
    string_map = {key.as_string(): (key, value) for key, value in combined_dict}
    # Return to dictionary
    return dict(string_map.values())


def _query_map(node, context):
    """query all values based on Dict.items"""
    values = {}
    for name, value in node.items:
        if isinstance(name, nodes.DictUnpack):
            double_starred = helpers.safe_query(value, context)
            if not double_starred:
                raise exceptions.InferenceError
            if not isinstance(double_starred, nodes.Dict):
                raise exceptions.InferenceError(node=node, context=context)
            unpack_items = _query_map(double_starred, context)
            values = _update_with_replacement(values, unpack_items)
        else:
            key = helpers.safe_query(name, context=context)
            value = helpers.safe_query(value, context=context)
            if any(not elem for elem in (key, value)):
                raise exceptions.InferenceError(node=node, context=context)
            values = _update_with_replacement(values, {key: value})
    return values


nodes.Dict._query = query_map


def _higher_function_scope(node):
    """ Search for the first function which encloses the given
    scope. This can be used for looking up in that function's
    scope, in case looking up in a lower scope for a particular
    name fails.

    :param node: A scope node.
    :returns:
        ``None``, if no parent function scope was found,
        otherwise an instance of :class:`astroid.scoped_nodes.Function`,
        which encloses the given node.
    """
    current = node
    while current.parent and not isinstance(current.parent, nodes.FunctionDef):
        current = current.parent
    if current and current.parent:
        return current.parent
    return None


def query_name(self, context=None):
    """query a Name: use name lookup rules"""
    frame, stmts = self.lookup(self.name)
    if not stmts:
        # Try to see if the name is enclosed in a nested function
        # and use the higher (first function) scope for searching.
        parent_function = _higher_function_scope(self.scope())
        if parent_function:
            _, stmts = parent_function.lookup(self.name)

        if not stmts:
            raise exceptions.NameInferenceError(
                name=self.name, scope=self.scope(), context=context
            )
    context = contextmod.copy_context(context)
    context.lookupname = self.name
    return bases._query_stmts(stmts, context, frame)


# pylint: disable=no-value-for-parameter
nodes.Name._query = decorators.raise_if_nothing_queried(
    decorators.path_wrapper(query_name)
)
nodes.AssignName.query_lhs = query_name  # won't work with a path wrapper


@decorators.raise_if_nothing_queried
@decorators.path_wrapper
def query_call(self: nodes.Call, context=None):
    """query a Call node by trying to guess what the function returns"""
    callcontext = contextmod.copy_context(context)
    callcontext.callcontext = contextmod.CallContext(
        args=self.args, keywords=self.keywords
    )
    callcontext.boundnode = None
    if context is not None:
        # extra context saved the args, what's for?
        callcontext.extra_context = _populate_context_lookup(self, context.clone())

    for callee in self.func.query(context):
        if callee is util.Uninferable:
            yield callee
            continue
        if callee is util.Unqueryable:
            if not isinstance(self.keywords, None):
                # not implemented
                print(kv)
                assert False
            return self.args

        try:
            if hasattr(callee, "query_call_result"):
                # no filed sensitive here
                yield from callee.query_call_result(caller=self, context=callcontext)
        except exceptions.InferenceError:
            continue
    return dict(node=self, context=context)


nodes.Call._query = query_call


@decorators.raise_if_nothing_queried
@decorators.path_wrapper
def query_import(self, context=None, asname=True):
    """query an Import node: return the imported module/object"""
    # do conservative query for all import statements
    yield util.Unqueryable


nodes.Import._query = query_import


@decorators.raise_if_nothing_queried
@decorators.path_wrapper
def query_import_from(self, context=None, asname=True):
    """query a ImportFrom node: return the imported module/object"""
    # do conservative query for all import statements
    yield util.Unqueryable



nodes.ImportFrom._query = query_import_from


def query_attribute(self, context=None):
    """query an Attribute node by using getattr on the associated object"""
    for owner in self.expr.query(context):
        if owner is util.Uninferable:
            yield owner
            continue

        if owner is util.Unqueryable:
            yield owner
            continue

        if context and context.boundnode:
            # This handles the situation where the attribute is accessed through a subclass
            # of a base class and the attribute is defined at the base class's level,
            # by taking in consideration a redefinition in the subclass.
            if isinstance(owner, bases.Instance) and isinstance(
                context.boundnode, bases.Instance
            ):
                try:
                    if helpers.is_subtype(
                        helpers.object_type(context.boundnode),
                        helpers.object_type(owner),
                    ):
                        owner = context.boundnode
                except exceptions._NonDeducibleTypeHierarchy:
                    # Can't determine anything useful.
                    pass
        elif not context:
            context = contextmod.InferenceContext()

        try:
            context.boundnode = owner
            yield from owner.igetattr(self.attrname, context)
        except (
            exceptions.AttributeInferenceError,
            exceptions.InferenceError,
            AttributeError,
        ):
            pass
        finally:
            context.boundnode = None
    return dict(node=self, context=context)


nodes.Attribute._query = decorators.raise_if_nothing_queried(
    decorators.path_wrapper(query_attribute)
)
# won't work with a path wrapper
nodes.AssignAttr.query_lhs = decorators.raise_if_nothing_queried(query_attribute)


@decorators.raise_if_nothing_queried
@decorators.path_wrapper
def query_global(self, context=None):
    if context.lookupname is None:
        raise exceptions.InferenceError(node=self, context=context)
    try:
        return bases._query_stmts(self.root().getattr(context.lookupname), context)
    except exceptions.AttributeInferenceError as error:
        raise exceptions.InferenceError(
            error.message, target=self, attribute=context.lookupname, context=context
        ) from error


nodes.Global._query = query_global


_SUBSCRIPT_SENTINEL = object()


def query_subscript(self: nodes.Subscript, context=None):
    """Inference for subscripts

    We're understanding if the index is a Const
    or a slice, passing the result of Inference
    to the value's `getitem` method, which should
    handle each supported index type accordingly.
    """

    found_one = False
    for value in self.value.query(context):
        if value is util.Uninferable:
            yield util.Uninferable
            return None
        for index in self.slice.query(context):
            if index is util.Uninferable:
                yield util.Uninferable
                return None

            # Try to deduce the index value.
            index_value = _SUBSCRIPT_SENTINEL
            if value.__class__ == bases.Instance:
                index_value = index
            elif index.__class__ == bases.Instance:
                instance_as_index = helpers.class_instance_as_index(index)
                if instance_as_index:
                    index_value = instance_as_index
            else:
                index_value = index

            if index_value is _SUBSCRIPT_SENTINEL:
                raise exceptions.InferenceError(node=self, context=context)

            try:
                assigned = value.getitem(index_value, context)
                # print(assigned)
            except (
                exceptions.AstroidTypeError,
                exceptions.AstroidIndexError,
                exceptions.AttributeInferenceError,
                AttributeError,
            ) as exc:
                raise exceptions.InferenceError(node=self, context=context) from exc

            # Prevent queriing if the queried subscript
            # is the same as the original subscripted object.
            if self is assigned or assigned is util.Uninferable:
                yield util.Uninferable
                return None
            yield from assigned.query(context)
            found_one = True

    if found_one:
        return dict(node=self, context=context)
    return None


nodes.Subscript._query = decorators.raise_if_nothing_queried(
    decorators.path_wrapper(query_subscript)
)
nodes.Subscript.query_lhs = decorators.raise_if_nothing_queried(query_subscript)


@decorators.raise_if_nothing_queried
@decorators.path_wrapper
def _query_boolop(self, context=None):
    """query a boolean operation (and / or / not).

    The function will calculate the boolean operation
    for all pairs generated through Inference for each component
    node.
    """
    values = self.values
    if self.op == "or":
        predicate = operator.truth
    else:
        predicate = operator.not_

    try:
        values = [value.query(context=context) for value in values]
    except exceptions.InferenceError:
        yield util.Uninferable
        return None

    for pair in itertools.product(*values):
        if any(item is util.Uninferable for item in pair):
            # Can't query the final result, just yield Uninferable.
            yield util.Uninferable
            continue

        bool_values = [item.bool_value() for item in pair]
        if any(item is util.Uninferable for item in bool_values):
            # Can't query the final result, just yield Uninferable.
            yield util.Uninferable
            continue

        # Since the boolean operations are short circuited operations,
        # this code yields the first value for which the predicate is True
        # and if no value respected the predicate, then the last value will
        # be returned (or Uninferable if there was no last value).
        # This is conforming to the semantics of `and` and `or`:
        #   1 and 0 -> 1
        #   0 and 1 -> 0
        #   1 or 0 -> 1
        #   0 or 1 -> 1
        value = util.Uninferable
        for value, bool_value in zip(pair, bool_values):
            if predicate(bool_value):
                yield value
                break
        else:
            yield value

    return dict(node=self, context=context)


nodes.BoolOp._query = _query_boolop


# UnaryOp, BinOp and AugAssign Inferences


def _filter_operation_errors(self, query_callable, context, error):
    for result in query_callable(self, context):
        if isinstance(result, error):
            # For the sake of .query(), we don't care about operation
            # errors, which is the job of pylint. So return something
            # which shows that we can't query the result.
            yield util.Uninferable
        else:
            yield result


def _query_unaryop(self, context=None):
    """query what an UnaryOp should return when evaluated."""
    for operand in self.operand.query(context):
        try:
            yield operand.query_unary_op(self.op)
        except TypeError as exc:
            # The operand doesn't support this operation.
            yield util.BadUnaryOperationMessage(operand, self.op, exc)
        except AttributeError as exc:
            meth = protocols.UNARY_OP_METHOD[self.op]
            if meth is None:
                # `not node`. Determine node's boolean
                # value and negate its result, unless it is
                # Uninferable, which will be returned as is.
                bool_value = operand.bool_value()
                if bool_value is not util.Uninferable:
                    yield nodes.const_factory(not bool_value)
                else:
                    yield util.Uninferable
            else:
                if not isinstance(operand, (bases.Instance, nodes.ClassDef)):
                    # The operation was used on something which
                    # doesn't support it.
                    yield util.BadUnaryOperationMessage(operand, self.op, exc)
                    continue

                try:
                    try:
                        methods = dunder_lookup.lookup(operand, meth)
                    except exceptions.AttributeInferenceError:
                        yield util.BadUnaryOperationMessage(operand, self.op, exc)
                        continue

                    meth = methods[0]
                    queried = next(meth.query(context=context))
                    if queried is util.Uninferable or not queried.callable():
                        continue

                    context = contextmod.copy_context(context)
                    context.callcontext = contextmod.CallContext(args=[operand])
                    call_results = queried.query_call_result(self, context=context)
                    result = next(call_results, None)
                    if result is None:
                        # Failed to query, return the same type.
                        yield operand
                    else:
                        yield result
                except exceptions.AttributeInferenceError as exc:
                    # The unary operation special method was not found.
                    yield util.BadUnaryOperationMessage(operand, self.op, exc)
                except exceptions.InferenceError:
                    yield util.Uninferable


@decorators.raise_if_nothing_queried
@decorators.path_wrapper
def query_unaryop(self, context=None):
    """query what an UnaryOp should return when evaluated."""
    yield from _filter_operation_errors(
        self, _query_unaryop, context, util.BadUnaryOperationMessage
    )
    return dict(node=self, context=context)


nodes.UnaryOp._query_unaryop = _query_unaryop
nodes.UnaryOp._query = query_unaryop


def _is_not_implemented(const):
    """Check if the given const node is NotImplemented."""
    return isinstance(const, nodes.Const) and const.value is NotImplemented


def _invoke_binop_Inference(instance, opnode, op, other, context, method_name):
    """Invoke binary operation Inference on the given instance."""
    methods = dunder_lookup.lookup(instance, method_name)
    context = contextmod.bind_context_to_node(context, instance)
    method = methods[0]
    queried = next(method.query(context=context))
    if queried is util.Uninferable:
        raise exceptions.InferenceError
    return instance.query_binary_op(opnode, op, other, context, queried)


def _aug_op(instance, opnode, op, other, context, reverse=False):
    """Get an Inference callable for an augmented binary operation."""
    method_name = protocols.AUGMENTED_OP_METHOD[op]
    return functools.partial(
        _invoke_binop_Inference,
        instance=instance,
        op=op,
        opnode=opnode,
        other=other,
        context=context,
        method_name=method_name,
    )


def _bin_op(instance, opnode, op, other, context, reverse=False):
    """Get an Inference callable for a normal binary operation.

    If *reverse* is True, then the reflected method will be used instead.
    """
    if reverse:
        method_name = protocols.REFLECTED_BIN_OP_METHOD[op]
    else:
        method_name = protocols.BIN_OP_METHOD[op]
    return functools.partial(
        _invoke_binop_Inference,
        instance=instance,
        op=op,
        opnode=opnode,
        other=other,
        context=context,
        method_name=method_name,
    )


def _get_binop_contexts(context, left, right):
    """Get contexts for binary operations.

    This will return two Inference contexts, the first one
    for x.__op__(y), the other one for y.__rop__(x), where
    only the arguments are inversed.
    """
    # The order is important, since the first one should be
    # left.__op__(right).
    for arg in (right, left):
        new_context = context.clone()
        new_context.callcontext = contextmod.CallContext(args=[arg])
        new_context.boundnode = None
        yield new_context


def _same_type(type1, type2):
    """Check if type1 is the same as type2."""
    return type1.qname() == type2.qname()


def _get_binop_flow(
    left, left_type, binary_opnode, right, right_type, context, reverse_context
):
    """Get the flow for binary operations.

    The rules are a bit messy:

        * if left and right have the same type, then only one
          method will be called, left.__op__(right)
        * if left and right are unrelated typewise, then first
          left.__op__(right) is tried and if this does not exist
          or returns NotImplemented, then right.__rop__(left) is tried.
        * if left is a subtype of right, then only left.__op__(right)
          is tried.
        * if left is a supertype of right, then right.__rop__(left)
          is first tried and then left.__op__(right)
    """
    op = binary_opnode.op
    if _same_type(left_type, right_type):
        methods = [_bin_op(left, binary_opnode, op, right, context)]
    elif helpers.is_subtype(left_type, right_type):
        methods = [_bin_op(left, binary_opnode, op, right, context)]
    elif helpers.is_supertype(left_type, right_type):
        methods = [
            _bin_op(right, binary_opnode, op, left, reverse_context, reverse=True),
            _bin_op(left, binary_opnode, op, right, context),
        ]
    else:
        methods = [
            _bin_op(left, binary_opnode, op, right, context),
            _bin_op(right, binary_opnode, op, left, reverse_context, reverse=True),
        ]
    return methods


def _get_aug_flow(
    left, left_type, aug_opnode, right, right_type, context, reverse_context
):
    """Get the flow for augmented binary operations.

    The rules are a bit messy:

        * if left and right have the same type, then left.__augop__(right)
          is first tried and then left.__op__(right).
        * if left and right are unrelated typewise, then
          left.__augop__(right) is tried, then left.__op__(right)
          is tried and then right.__rop__(left) is tried.
        * if left is a subtype of right, then left.__augop__(right)
          is tried and then left.__op__(right).
        * if left is a supertype of right, then left.__augop__(right)
          is tried, then right.__rop__(left) and then
          left.__op__(right)
    """
    bin_op = aug_opnode.op.strip("=")
    aug_op = aug_opnode.op
    if _same_type(left_type, right_type):
        methods = [
            _aug_op(left, aug_opnode, aug_op, right, context),
            _bin_op(left, aug_opnode, bin_op, right, context),
        ]
    elif helpers.is_subtype(left_type, right_type):
        methods = [
            _aug_op(left, aug_opnode, aug_op, right, context),
            _bin_op(left, aug_opnode, bin_op, right, context),
        ]
    elif helpers.is_supertype(left_type, right_type):
        methods = [
            _aug_op(left, aug_opnode, aug_op, right, context),
            _bin_op(right, aug_opnode, bin_op, left, reverse_context, reverse=True),
            _bin_op(left, aug_opnode, bin_op, right, context),
        ]
    else:
        methods = [
            _aug_op(left, aug_opnode, aug_op, right, context),
            _bin_op(left, aug_opnode, bin_op, right, context),
            _bin_op(right, aug_opnode, bin_op, left, reverse_context, reverse=True),
        ]
    return methods


def _query_binary_operation(left, right, binary_opnode, context, flow_factory):
    """query a binary operation between a left operand and a right operand

    This is used by both normal binary operations and augmented binary
    operations, the only difference is the flow factory used.
    """

    context, reverse_context = _get_binop_contexts(context, left, right)
    left_type = helpers.object_type(left)
    right_type = helpers.object_type(right)
    methods = flow_factory(
        left, left_type, binary_opnode, right, right_type, context, reverse_context
    )
    for method in methods:
        try:
            results = list(method())
        except AttributeError:
            continue
        except exceptions.AttributeInferenceError:
            import traceback
            traceback.print_exc()
            continue
        except exceptions.InferenceError:
            yield util.Uninferable
            return
        else:
            if any(result is util.Uninferable for result in results):
                yield util.Uninferable
                return

            if all(map(_is_not_implemented, results)):
                continue
            not_implemented = sum(
                1 for result in results if _is_not_implemented(result)
            )
            if not_implemented and not_implemented != len(results):
                # Can't query yet what this is.
                yield util.Uninferable
                return

            yield from results
            return
    # The operation doesn't seem to be supported so let the caller know about it
    yield util.BadBinaryOperationMessage(left_type, binary_opnode.op, right_type)


def _query_binop(self, context):
    """Binary operation Inference logic."""
    left = self.left
    right = self.right

    # we use two separate contexts for evaluating lhs and rhs because
    # 1. evaluating lhs may leave some undesired entries in context.path
    #    which may not let us query right value of rhs
    context = context or contextmod.InferenceContext()
    lhs_context = contextmod.copy_context(context)
    rhs_context = contextmod.copy_context(context)
    lhs_iter = left.query(context=lhs_context)
    rhs_iter = right.query(context=rhs_context)
    for lhs, rhs in itertools.product(lhs_iter, rhs_iter):
        if any(value is util.Uninferable for value in (rhs, lhs)):
            # Don't know how to process this.
            yield util.Uninferable
            return

        try:
            yield from _query_binary_operation(lhs, rhs, self, context, _get_binop_flow)
        except exceptions._NonDeducibleTypeHierarchy:
            yield util.Uninferable


@decorators.yes_if_nothing_queried
@decorators.path_wrapper
def query_binop(self, context=None):
    return _filter_operation_errors(
        self, _query_binop, context, util.BadBinaryOperationMessage
    )


nodes.BinOp._query_binop = _query_binop
nodes.BinOp._query = query_binop


def _query_augassign(self, context=None):
    """Inference logic for augmented binary operations."""
    if context is None:
        context = contextmod.InferenceContext()

    rhs_context = context.clone()

    lhs_iter = self.target.query_lhs(context=context)
    rhs_iter = self.value.query(context=rhs_context)
    for lhs, rhs in itertools.product(lhs_iter, rhs_iter):
        if any(value is util.Uninferable for value in (rhs, lhs)):
            # Don't know how to process this.
            yield util.Uninferable
            return

        try:
            yield from _query_binary_operation(
                left=lhs,
                right=rhs,
                binary_opnode=self,
                context=context,
                flow_factory=_get_aug_flow,
            )
        except exceptions._NonDeducibleTypeHierarchy:
            yield util.Uninferable


@decorators.raise_if_nothing_queried
@decorators.path_wrapper
def query_augassign(self, context=None):
    return _filter_operation_errors(
        self, _query_augassign, context, util.BadBinaryOperationMessage
    )


nodes.AugAssign._query_augassign = _query_augassign
nodes.AugAssign._query = query_augassign

# End of binary operation Inference.


@decorators.raise_if_nothing_queried
def query_arguments(self, context=None):
    name = context.lookupname
    if name is None:
        raise exceptions.InferenceError(node=self, context=context)
    return protocols._arguments_query_argname(self, name, context)


nodes.Arguments._query = query_arguments


@decorators.raise_if_nothing_queried
@decorators.path_wrapper
def query_assign(self, context=None):
    """query a AssignName/AssignAttr: need to inspect the RHS part of the
    assign node
    """
    if isinstance(self.parent, nodes.AugAssign):
        return self.parent.query(context)

    stmts = list(self.assigned_stmts(context=context))
    return bases._query_stmts(stmts, context)


nodes.AssignName._query = query_assign
nodes.AssignAttr._query = query_assign


@decorators.raise_if_nothing_queried
@decorators.path_wrapper
def query_empty_node(self, context=None):
    if not self.has_underlying_object():
        yield util.Uninferable
    else:
        try:
            yield from MANAGER.query_ast_from_something(self.object, context=context)
        except exceptions.AstroidError:
            yield util.Uninferable


nodes.EmptyNode._query = query_empty_node


@decorators.raise_if_nothing_queried
def query_index(self, context=None):
    return self.value.query(context)


nodes.Index._query = query_index

# TODO: move directly into bases.Instance when the dependency hell
# will be solved.
def instance_getitem(self, index, context=None):
    # Rewrap index to Const for this case
    new_context = contextmod.bind_context_to_node(context, self)
    if not context:
        context = new_context

    # Create a new callcontext for providing index as an argument.
    new_context.callcontext = contextmod.CallContext(args=[index])

    method = next(self.igetattr("__getitem__", context=context), None)
    if not isinstance(method, bases.BoundMethod):
        raise exceptions.InferenceError(
            "Could not find __getitem__ for {node!r}.", node=self, context=context
        )

    return next(method.query_call_result(self, new_context))


bases.Instance.getitem = instance_getitem


def _populate_context_lookup(call, context):
    # Allows context to be saved for later
    # for Inference inside a function
    context_lookup = {}
    if context is None:
        return context_lookup
    for arg in call.args:
        if isinstance(arg, nodes.Starred):
            context_lookup[arg.value] = context
        else:
            context_lookup[arg] = context
    keywords = call.keywords if call.keywords is not None else []
    for keyword in keywords:
        context_lookup[keyword.value] = context
    return context_lookup


@decorators.raise_if_nothing_queried
def query_ifexp(self, context=None):
    """Support IfExp Inference

    If we can't query the truthiness of the condition, we default
    to queriing both branches. Otherwise, we query either branch
    depending on the condition.
    """
    both_branches = False
    # We use two separate contexts for evaluating lhs and rhs because
    # evaluating lhs may leave some undesired entries in context.path
    # which may not let us query right value of rhs.

    context = context or contextmod.InferenceContext()
    lhs_context = contextmod.copy_context(context)
    rhs_context = contextmod.copy_context(context)
    try:
        test = next(self.test.query(context=context.clone()))
    except exceptions.InferenceError:
        both_branches = True
    else:
        if test is not util.Uninferable:
            if test.bool_value():
                yield from self.body.query(context=lhs_context)
            else:
                yield from self.orelse.query(context=rhs_context)
        else:
            both_branches = True
    if both_branches:
        yield from self.body.query(context=lhs_context)
        yield from self.orelse.query(context=rhs_context)


nodes.IfExp._query = query_ifexp


# pylint: disable=dangerous-default-value
@wrapt.decorator
def _cached_generator(func, instance, args, kwargs, _cache={}):
    node = args[0]
    try:
        return iter(_cache[func, id(node)])
    except KeyError:
        result = func(*args, **kwargs)
        # Need to keep an iterator around
        original, copy = itertools.tee(result)
        _cache[func, id(node)] = list(copy)
        return original


# When queriing a property, we instantiate a new `objects.Property` object,
# which in turn, because it inherits from `FunctionDef`, sets itself in the locals
# of the wrapping frame. This means that everytime we query a property, the locals
# are mutated with a new instance of the property. This is why we cache the result
# of the function's Inference.
@_cached_generator
def query_functiondef(self, context=None):
    if not self.decorators or not bases._is_property(self):
        yield self
        return dict(node=self, context=context)

    prop_func = objects.Property(
        function=self,
        name=self.name,
        doc=self.doc,
        lineno=self.lineno,
        parent=self.parent,
        col_offset=self.col_offset,
    )
    prop_func.postinit(body=[], args=self.args)
    yield prop_func
    return dict(node=self, context=context)


nodes.FunctionDef._query = query_functiondef

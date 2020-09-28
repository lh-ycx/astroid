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
from astroid import query_protocols
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
    return [self]

# def query_lambda(self, context=None):
#     return self.body.query(context)


nodes.Module._query = query_end
nodes.ClassDef._query = query_end
nodes.Lambda._query = query_end
nodes.Const._query = query_end
nodes.Slice._query = query_end


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

    res = []
    for callee in self.func.query(context):
        if callee is util.Unqueryable:
            if self.keywords is not None:
                # not implemented
                print(self.keywords)
                assert False
            for arg in self.args:
                res.extend(arg.query(context))
            continue

        if hasattr(callee, "query_end") and callee.query_end:
            res.append(callee)
            continue

        try:
            if hasattr(callee, "query_call_result"):
                for result in callee.query_call_result(caller=self, context=callcontext):
                    if result is util.Unqueryable:
                        for arg in self.args:
                            res.extend(arg.query(context))
                    else:
                        res.append(result)

        except exceptions.InferenceError:
            continue
    return res

nodes.Call._query = query_call

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
    res = []
    for stmt in stmts:
        res.extend(_query_stmt(stmt, context, frame))
    return res


nodes.Name._query = query_name
nodes.AssignName.query_lhs = query_name


def _query_stmt(stmt, context, frame=None):
    queried = False
    if context is not None:
        name = context.lookupname
        context = context.clone()
    else:
        name = None
        context = contextmod.InferenenceContext()

    res = []

    if stmt is util.Uninferable:
        assert False
    context.lookupname = stmt._query_name(frame, name)
    try:
        res.extend(stmt.query(context=context))
        queried = True
    except exceptions.NameInferenceError:
        assert False
        # continue
    except exceptions.InferenceError:
        assert False
        # yield util.Uninferable
        # queried = True
        # continue

    if not queried:
        raise exceptions.InferenceError(
            "query failed for all members of {stmt!r}.",
            stmts=stmt,
            frame=frame,
            context=context,
        )
    return res


# When inferring a property, we instantiate a new `objects.Property` object,
# which in turn, because it inherits from `FunctionDef`, sets itself in the locals
# of the wrapping frame. This means that everytime we query a property, the locals
# are mutated with a new instance of the property. This is why we cache the result
# of the function's Inference.
def query_functiondef(self, context=None):
    if not self.decorators or not bases._is_property(self):
        return [self]

    prop_func = objects.Property(
        function=self,
        name=self.name,
        doc=self.doc,
        lineno=self.lineno,
        parent=self.parent,
        col_offset=self.col_offset,
    )
    prop_func.postinit(body=[], args=self.args)
    return [prop_func]


nodes.FunctionDef._query = query_functiondef


def query_assign(self, context=None):
    """query a AssignName/AssignAttr: need to inspect the RHS part of the
    assign node
    """
    if isinstance(self.parent, nodes.AugAssign):
        return self.parent.query(context)

    stmts = self.query_assigned_stmts(context=context)

    # can't find further dependency, roll back
    res = []
    for stmt in stmts:
        if stmt.query_end:
            res.append(stmt)
        else:
            res.extend(_query_stmt(stmt, context))
    return res

nodes.AssignName._query = query_assign
nodes.AssignAttr._query = query_assign

def query_attribute(self, context=None):
    """query an Attribute node by using getattr on the associated object"""
    res = []
    for owner in self.expr.query(context):
        if owner is util.Uninferable:
            assert False

        if owner is util.Unqueryable:
            continue

        if owner.query_end:
            res.extend([owner, util.Unqueryable])
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
            res.extend(owner.query_attr(self.attrname, context))
        except (
            exceptions.AttributeInferenceError,
            exceptions.InferenceError,
            AttributeError,
        ):
            import traceback
            traceback.print_exc()
            pass
        finally:
            context.boundnode = None
    if len(res) == 0:
        return [util.Unqueryable]
    return res


nodes.Attribute._query = query_attribute
nodes.AssignAttr.query_lhs = query_attribute


def query_import(self, context=None, asname=True):
    """query an Import node: return the imported module/object"""
    # do conservative query for all import statements
    return [util.Unqueryable]


nodes.Import._query = query_import

# e.g. [char(x) for x in row] => query(row)
def query_generator_expr(self: nodes.GeneratorExp, context=None):
    elements = self.elt.query(context)
    return elements
    # for ele in elements:

nodes.GeneratorExp._query = query_generator_expr

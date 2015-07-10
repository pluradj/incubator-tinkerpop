/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.groovy.jsr223

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Graph
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.MethodNode
import org.codehaus.groovy.ast.Parameter
import org.codehaus.groovy.ast.expr.Expression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.transform.stc.ExtensionMethodNode
import org.codehaus.groovy.transform.stc.GroovyTypeCheckingExtensionSupport

import java.util.function.BiPredicate

/**
 * A sandbox for the {@link GremlinGroovyScriptEngine} that provides base functionality for securing evaluated scripts.
 * By default, this implementation ensures that the variable "graph" is always a {@link Graph} instance and the
 * variable "g" is always a {@link GraphTraversalSource}.
 * <p/>
 * Users should typically extend this class to modify features.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class SandboxExtension extends GroovyTypeCheckingExtensionSupport.TypeCheckingDSL {

    public static final BiPredicate<VariableExpression, Map<String,ClassNode>> VARIABLES_ALLOW_ALL = { var, types -> true }

    public static final BiPredicate<VariableExpression, Map<String,ClassNode>> METHODS_ALLOW_ALL = { exp, method -> true }
    public static final BiPredicate<Expression, MethodNode> METHODS_REGEX_WHITELIST = { exp, method ->
        def descriptor = toMethodDescriptor(method)
        !methodList.any { descriptor =~ it }
    }
    public static final BiPredicate<Expression, MethodNode> METHODS_REGEX_BLACKLIST = { exp, method -> !METHODS_REGEX_WHITELIST.test(exp, method) }

    protected boolean graphIsAlwaysGraphInstance = true
    protected boolean gIsAlwaysGraphTraversalSource = true
    protected BiPredicate<VariableExpression, Map<String,ClassNode>> variableFilter = VARIABLES_ALLOW_ALL
    protected BiPredicate<Expression, MethodNode> methodFilter = METHODS_ALLOW_ALL
    protected List<String> methodList = new ArrayList<String>();

    @Override
    Object run() {
        unresolvedVariable { var ->
            if (var.name == "graph" && graphIsAlwaysGraphInstance) {
                storeType(var, classNodeFor(Graph))
                handled = true
                return
            }

            if (var.name == "g" && gIsAlwaysGraphTraversalSource) {
                storeType(var, classNodeFor(GraphTraversalSource))
                handled = true
                return
            }

            final Map<String,ClassNode> varTypes = (Map<String,ClassNode>) GremlinGroovyScriptEngine.COMPILE_OPTIONS.get()
                    .get(GremlinGroovyScriptEngine.COMPILE_OPTIONS_VAR_TYPES)
            if (varTypes.containsKey(var.name) && variableFilter.test(var, varTypes))  {
                if (!(var.name in ["graph",  "g"]) || (var.name == "graph" && !graphIsAlwaysGraphInstance
                         || var.name == "g" && !gIsAlwaysGraphTraversalSource)) {
                    storeType(var, varTypes.get(var.name))
                    handled = true
                    return
                }
            }
        }

        onMethodSelection { expr, MethodNode methodNode ->
            if (!methodFilter.test(expr, methodNode))
                addStaticTypeError("Not authorized to call this method: $descr", expr)
        }
    }

    private static String prettyPrint(ClassNode node) {
        node.isArray()?"${prettyPrint(node.componentType)}[]":node.toString(false)
    }

    private static String toMethodDescriptor(final MethodNode node) {
        if (node instanceof ExtensionMethodNode)
            return toMethodDescriptor(node.extensionMethodNode)

        def sb = new StringBuilder()
        sb.append(node.declaringClass.toString(false))
        sb.append("#")
        sb.append(node.name)
        sb.append('(')
        sb.append(node.parameters.collect { Parameter it ->
            prettyPrint(it.originType)
        }.join(','))
        sb.append(')')
        sb
    }
}

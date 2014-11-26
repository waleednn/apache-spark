/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.linalg;

import static org.junit.Assert.*;
import org.junit.Test;

import java.io.Serializable;

public class JavaMatricesSuite implements Serializable {

    @Test
    public void randMatrixConstruction() {
        Matrix r = Matrices.rand(3, 4, 24);
        DenseMatrix dr = DenseMatrix.rand(3, 4, 24);
        assertArrayEquals(r.toArray(), dr.toArray(), 0.0);

        Matrix rn = Matrices.randn(3, 4, 24);
        DenseMatrix drn = DenseMatrix.randn(3, 4, 24);
        assertArrayEquals(rn.toArray(), drn.toArray(), 0.0);

        Matrix s = Matrices.sprand(3, 4, 0.5, 24);
        SparseMatrix sr = SparseMatrix.sprand(3, 4, 0.5, 24);
        assertArrayEquals(s.toArray(), sr.toArray(), 0.0);

        Matrix sn = Matrices.sprandn(3, 4, 0.5, 24);
        SparseMatrix srn = SparseMatrix.sprandn(3, 4, 0.5, 24);
        assertArrayEquals(sn.toArray(), srn.toArray(), 0.0);
    }

    @Test
    public void identityMatrixConstruction() {
        Matrix r = Matrices.eye(2);
        DenseMatrix dr = DenseMatrix.eye(2);
        SparseMatrix sr = SparseMatrix.speye(2);
        assertArrayEquals(r.toArray(), dr.toArray(), 0.0);
        assertArrayEquals(sr.toArray(), dr.toArray(), 0.0);
        assertArrayEquals(r.toArray(), new double[]{1.0, 0.0, 0.0, 1.0}, 0.0);
    }

    @Test
    public void diagonalMatrixConstruction() {
        Vector v = Vectors.dense(1.0, 0.0, 2.0);
        Vector sv = Vectors.sparse(3, new int[]{0, 2}, new double[]{1.0, 2.0});

        Matrix m = Matrices.diag(v);
        Matrix sm = Matrices.diag(sv);
        DenseMatrix d = DenseMatrix.diag(v);
        DenseMatrix sd = DenseMatrix.diag(sv);
        SparseMatrix s = SparseMatrix.diag(v);
        SparseMatrix ss = SparseMatrix.diag(sv);

        assertArrayEquals(m.toArray(), sm.toArray(), 0.0);
        assertArrayEquals(d.toArray(), sm.toArray(), 0.0);
        assertArrayEquals(d.toArray(), sd.toArray(), 0.0);
        assertArrayEquals(sd.toArray(), s.toArray(), 0.0);
        assertArrayEquals(s.toArray(), ss.toArray(), 0.0);
        assertArrayEquals(s.values(), ss.values(), 0.0);
        assert(s.values().length == 2);
        assert(ss.values().length == 2);
        assert(s.colPtrs().length == 2);
        assert(ss.colPtrs().length == 2);
    }

    @Test
    public void zerosMatrixConstruction() {
        Matrix z = Matrices.zeros(2, 2);
        Matrix one = Matrices.ones(2, 2);
        DenseMatrix dz = DenseMatrix.zeros(2, 2);
        DenseMatrix done = DenseMatrix.ones(2, 2);

        assertArrayEquals(z.toArray(), new double[]{0.0, 0.0, 0.0, 0.0}, 0.0);
        assertArrayEquals(dz.toArray(), new double[]{0.0, 0.0, 0.0, 0.0}, 0.0);
        assertArrayEquals(one.toArray(), new double[]{1.0, 1.0, 1.0, 1.0}, 0.0);
        assertArrayEquals(done.toArray(), new double[]{1.0, 1.0, 1.0, 1.0}, 0.0);
    }

    @Test
    public void concatenateMatrices() {
        int m = 3;
        int n = 2;

        SparseMatrix spMat1 = SparseMatrix.sprand(m, n, 0.5, 42);
        DenseMatrix deMat1 = DenseMatrix.rand(m, n, 42);
        Matrix deMat2 = Matrices.eye(3);
        Matrix spMat2 = Matrices.speye(3);
        Matrix deMat3 = Matrices.eye(2);
        Matrix spMat3 = Matrices.speye(2);

        Matrix spHorz = Matrices.horzcat(new Matrix[]{spMat1, spMat2});
        Matrix deHorz1 = Matrices.horzcat(new Matrix[]{deMat1, deMat2});
        Matrix deHorz2 = Matrices.horzcat(new Matrix[]{spMat1, deMat2});
        Matrix deHorz3 = Matrices.horzcat(new Matrix[]{deMat1, spMat2});

        assert(deHorz1.numRows() == 3);
        assert(deHorz2.numRows() == 3);
        assert(deHorz3.numRows() == 3);
        assert(spHorz.numRows() == 3);
        assert(deHorz1.numCols() == 5);
        assert(deHorz2.numCols() == 5);
        assert(deHorz3.numCols() == 5);
        assert(spHorz.numCols() == 5);

        Matrix spVert = Matrices.vertcat(new Matrix[]{spMat1, spMat3});
        Matrix deVert1 = Matrices.vertcat(new Matrix[]{deMat1, deMat3});
        Matrix deVert2 = Matrices.vertcat(new Matrix[]{spMat1, deMat3});
        Matrix deVert3 = Matrices.vertcat(new Matrix[]{deMat1, spMat3});

        assert(deVert1.numRows() == 5);
        assert(deVert2.numRows() == 5);
        assert(deVert3.numRows() == 5);
        assert(spVert.numRows() == 5);
        assert(deVert1.numCols() == 2);
        assert(deVert2.numCols() == 2);
        assert(deVert3.numCols() == 2);
        assert(spVert.numCols() == 2);
    }
}

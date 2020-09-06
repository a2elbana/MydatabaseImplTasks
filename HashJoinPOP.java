package dbis.toydb.qep.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dbis.toydb.algebra.Condition;
import dbis.toydb.algebra.RelAttr;
import dbis.toydb.qep.POP;
import dbis.toydb.qep.impl.helper_pops.BufferPOP;
import dbis.toydb.util.AttrInfo;
import dbis.toydb.util.DBException;

public class PartitionedHashJoinPOP extends POP {
	
	private RelAttr leftAtt;
	private RelAttr rightAtt;
	List<Object[]> joinedAttr = new ArrayList<>();
	int lastAtrrPos = -1;
	BufferPOP leftChild = new BufferPOP();
	BufferPOP rightChild = new BufferPOP();
	Map<Integer, ArrayList<Object[]>> leftAttrMap = new HashMap<>();
	Map<Integer, ArrayList<Object[]>> rightAttrMap = new HashMap<>();
	List<AsyncThread> threads = new ArrayList<AsyncThread>();
	public int leftTargetAttrPos;
	public int rightTargetAttrPos;

	/*
	 * The condition contains the attributes of the two relations on which the join
	 * should be calculated (userID)
	 */
	public PartitionedHashJoinPOP(Condition cond) {
		this.leftAtt = cond.getLHSAttr();
		this.rightAtt = cond.getRHSAttr();

		int i = 0;
		//start to add empty buckets
		while (i < 5) {
			this.leftAttrMap.put(i, new ArrayList<>());
			i++;
		}
		i = 0;
		while (i < 5) {
			this.rightAttrMap.put(i, new ArrayList<>());
			i++;
		}
	}

	/*
	 * This physical plan operator receives a left and right child that represents
	 * the two relations that should be joined by the join condition (userID). Hint:
	 * It is possible to use the dbis.toydb.qep.impl.helper_pops.BufferPOP class to
	 * operate on the two relations. Hint: You can also modify the BufferPOP or
	 * write your own. View the BufferPOP class the see which functions you can use!
	 */
	public void open() throws DBException {
		leftChild.setLeftChild(this.getLeftChild());
		rightChild.setLeftChild(this.getRightChild());
		leftChild.open();
		rightChild.open();
		setLeftRightTargetAttrPos();
		divideEntries();
		addThreads();
		startThreads();
		joinThreads();
	}
	
	/**
	 * set the position of the attribute, that will be joined
	 */
	private void setLeftRightTargetAttrPos() {
		for (int i = 0; i < leftChild.getAttributes().length; i++) {
			if (leftChild.getAttributes()[i].getAttrName().equals(leftAtt.getAttrName())) {
				leftTargetAttrPos = i;
				break;
			}
		}
		for (int i = 0; i < rightChild.getAttributes().length; i++) {
			if (rightChild.getAttributes()[i].getAttrName().equals(rightAtt.getAttrName())) {
				rightTargetAttrPos = i;
				break;
			}
		}
	}
	
	/**
	 * Divide the entries from left and right tables into leftMap and rightMap. 
	 * @throws DBException
	 */
	private void divideEntries() throws DBException {
		Object[] leftTuple;
		Object[] rightTuple;
		while ((leftTuple = leftChild.next()) != null) {
			ArrayList<Object[]> tempList = this.leftAttrMap.get((int) leftTuple[leftTargetAttrPos] % 5);
			tempList.add(leftTuple);
			this.leftAttrMap.put((int) leftTuple[leftTargetAttrPos] % 5, tempList);
		}
		while ((rightTuple = rightChild.next()) != null) {
			ArrayList<Object[]> tempList = this.rightAttrMap.get((int) rightTuple[rightTargetAttrPos] % 5);
			tempList.add(rightTuple);
			this.rightAttrMap.put((int) rightTuple[rightTargetAttrPos] % 5, tempList);
		}
	}
	
	/**
	 * create new AsyncThreads and add them to the list
	 */
	private void addThreads() {
		RunnableImpl insertRun = new RunnableImpl(this);
		for (int bucketPos = 0; bucketPos < 5; bucketPos++) {
			threads.add(new AsyncThread(insertRun, bucketPos));
		}
	}
	
	/**
	 * 
	 */
	private void startThreads() {
		for (Thread t : threads) {
			t.start();
		}
	}
	
	/**
	 * 
	 * @throws DBException
	 */
	private void joinThreads() throws DBException {
		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				System.err.println(e.getMessage());
				throw new DBException();
			}
		}
	}

	/**
	 * increment the position of last position of the attribute and return the attribute if 
	 * we didn't reached the last entry
	 */
	public Object[] next() throws DBException {
		lastAtrrPos++;
		return lastAtrrPos == this.joinedAttr.size() ? null : this.joinedAttr.get(lastAtrrPos);
	}

	public void close() throws DBException {
		this.leftChild.close();
		this.rightChild.close();
	}

	/*
	 * Returns the attribute info of the resulting schema. Since this is an inner
	 * join, attributes of both relations should be joined together. Schema of users
	 * relation: u_id, u_name Schema of orders relation: o_id, o_userID, o_product
	 * Should be joined together to u_id, u_name, o_id, o_userID, o_product Hint: If
	 * you use the BufferPOP you can retrieve the attributeInfos with
	 * BufferPOP.getAttributes()!
	 */
	public AttrInfo[] getAttributes() {
		List<AttrInfo> result = new ArrayList<>();
		for (AttrInfo attr : leftChild.getAttributes()) {
			result.add(attr);
		}
		for (AttrInfo attr : rightChild.getAttributes()) {
			result.add(attr);
		}
		return result.stream().toArray(AttrInfo[]::new);
	}

	/**
	 * add joinded attribute to the list
	 * @param obj
	 */
	public void addJoinedAttr(Object[] obj) {
		this.joinedAttr.add(obj);
	}

	public Map<Integer, ArrayList<Object[]>> getLeftAttrMap() {
		return leftAttrMap;
	}

	public Map<Integer, ArrayList<Object[]>> getRightAttrMap() {
		return rightAttrMap;
	}

	public int getLeftTargetAttrPos() {
		return leftTargetAttrPos;
	}

	public int getRightTargetAttrPos() {
		return rightTargetAttrPos;
	}

	
	

}

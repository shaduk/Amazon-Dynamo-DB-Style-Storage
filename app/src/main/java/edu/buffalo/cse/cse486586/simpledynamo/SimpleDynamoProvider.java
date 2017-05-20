package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {
	ConcurrentHashMap<String, String> globalStore = new ConcurrentHashMap<String, String>();
	ConcurrentHashMap<String, String> localDatabase = new ConcurrentHashMap<String, String>();
	String[] allPorts = {"11124", "11112", "11108", "11116", "11120"};
	static final int SERVER_PORT = 10000;
	private boolean wait = true;
	private int queryReceivedFrom = 0;
	private String thisNode = null;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("@")) {
			localDatabase.clear();
			//Log.v(TAG,"Deleted database");
		}
		else if(selection.equals("*")) {
			for(String port : allPorts) {
				MessageObj mDelete = new MessageObj();
				mDelete.reqT = MessageObj.reqType.DELETE;
				mDelete.fwdPort = port;
				mDelete.key = "*";
				newClient(mDelete);
			}
		} else {
			String thisKeybelongs = belongsWhere(selection);
			String[] deleteFrom = getReplicators(thisKeybelongs);
			for(String port: deleteFrom) {
				MessageObj mDelete = new MessageObj();
				mDelete.reqT = MessageObj.reqType.DELETE;
				mDelete.fwdPort = port;
				mDelete.key = selection;
				newClient(mDelete);
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}
	//

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		synchronized (this) {

			// TODO Auto-generated method stub
			String key = values.getAsString("key");
			String value = values.getAsString("value");
			//Log.v("Does is belong here?", String.valueOf(belongsHere(key)));
			String keyBelongsTo = belongsWhere(key);
			String[] replicateTo = getReplicators(keyBelongsTo);

			for (String port : replicateTo) {
				Log.v(key + "should r to", String.valueOf(Integer.valueOf(port)));
				MessageObj mInsert = new MessageObj();
				mInsert.key = key;
				mInsert.val = value;
				mInsert.reqT = MessageObj.reqType.INSERT;
				mInsert.fwdPort = port;
				mInsert.comingFrom = thisNode;
				newClient(mInsert);
			}
			return uri;
		}
	}

	String[] getReplicators(String node) {
		String[] ports = {node, returnNodeSucc(node,1), returnNodeSucc(node, 2)};
		return ports;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		synchronized (this) {
			MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
			if (selection.equals("*")) {

				globalStore = new ConcurrentHashMap<String, String>();
				for (String port : allPorts) {
					MessageObj mQuery = new MessageObj();
					mQuery.fwdPort = port; //Returns the next node of current Node
					mQuery.store = new ConcurrentHashMap<String, String>();
					mQuery.comingFrom = thisNode;
					mQuery.reqT = MessageObj.reqType.QUERY_ALL;
					newClient(mQuery);
				}
				while (wait) {
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				wait = true;
				for (Map.Entry<String, String> entry : globalStore.entrySet()) {
					matrixCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
				}
				globalStore.clear();
				return matrixCursor;

			} else if (selection.equals("@")) {

				for (Map.Entry<String, String> entry : localDatabase.entrySet()) {
					matrixCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
				}
			} else {
				String selectionBelongsTo = belongsWhere(selection);
				String[] replicators = getReplicators(selectionBelongsTo);
				for (String port : replicators) {
					MessageObj mQuery = new MessageObj();
					mQuery.fwdPort = port;
					mQuery.key = selection;
					mQuery.reqT = MessageObj.reqType.QUERY_ONE;
					mQuery.comingFrom = thisNode;
					newClient(mQuery);
				}

				while (!globalStore.containsKey(selection)) {
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				matrixCursor.addRow(new String[]{selection, globalStore.get(selection)});
				globalStore.clear();
			}
			return matrixCursor;
		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
			localDatabase.clear();
			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
			thisNode = myPort;

			try {
				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			} catch (IOException e) {
				//Log.e(TAG, "Can't create a ServerSocket");
				return false;
			}

			SharedPreferences isFailed = this.getContext().getSharedPreferences("failed", 0);
			if (isFailed.getBoolean("start", true)) {
				isFailed.edit().putBoolean("start", false).commit();
			} else {
				StartRecovery();

			}
			return true;
	}

	public void StartRecovery() {

		globalStore = new ConcurrentHashMap<String, String>();
		MessageObj recover1 = new MessageObj();
		recover1.fwdPort = returnNodePred(thisNode, 2);
		recover1.reqT = MessageObj.reqType.RECOVERY;
		recover1.comingFrom = thisNode;
		recover1.store = new ConcurrentHashMap<String, String>();
		newClient(recover1);
		MessageObj recover = new MessageObj();
		recover.fwdPort = returnNodePred(thisNode, 1);
		recover.reqT = MessageObj.reqType.RECOVERY;
		recover.comingFrom = thisNode;
		recover.store = new ConcurrentHashMap<String, String>();
		newClient(recover);
		MessageObj recover2 = new MessageObj();
		recover2.fwdPort = returnNodeSucc(thisNode, 1);
		recover2.reqT = MessageObj.reqType.RECOVERY;
		recover2.comingFrom = thisNode;
		recover2.store = new ConcurrentHashMap<String, String>();
		newClient(recover2);
	}

	public void newClient(MessageObj messageObj)
	{
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageObj);
		return;
	}


	public String belongsWhere(String key)
	{
		String[] nodes = {"11124", "11112", "11108", "11116", "11120"};
		String returnVal = "";
		int nodesLength = nodes.length;
		for ( int i = 0; i < nodesLength; i++) {
			String currNode = nodes[i];
			int ind = (i-1)%nodesLength;
			if(ind < 0) ind += nodesLength;
			String prevNode = nodes[ind];
			String prevNodeId = null;
			String keyHash = null;
			String thisNodeId = null;
			try {
				keyHash = genHash(key);
				thisNodeId = genHash(String.valueOf((Integer.parseInt(currNode) / 2)));
				//Log.v("hash of node", thisNodeId + "-" + thisNode + "-"+String.valueOf((Integer.parseInt(thisNode)/2)));
				prevNodeId = genHash(String.valueOf((Integer.parseInt(prevNode) / 2)));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			if(keyHash.compareTo(prevNodeId) > 0 && keyHash.compareTo(thisNodeId) <= 0 && prevNodeId.compareTo(thisNodeId) < 0)
				returnVal = currNode;
			else if(keyHash.compareTo(thisNodeId) >= 0 && keyHash.compareTo(prevNodeId) > 0 && prevNodeId.compareTo(thisNodeId) > 0)
				returnVal = currNode;
			else if(keyHash.compareTo(thisNodeId) <= 0 && keyHash.compareTo(prevNodeId) < 0 && thisNodeId.compareTo(prevNodeId) < 0)
				returnVal = currNode;
		}
		return returnVal;
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private String returnNodeSucc(String node, int n) {
		String[] nodes = {"11124", "11112", "11108", "11116", "11120"};
		String returnVal = "";
		for ( int i = 0; i < nodes.length; i++) {
			if(nodes[i].equals(node)){
				returnVal = nodes[(i+n)%nodes.length];
			}
		}
		return returnVal;
	}

	private String returnNodePred(String node, int n) {
		String[] nodes = {"11124", "11112", "11108", "11116", "11120"};
		String returnVal = "";
		int nodesLength = nodes.length;
		for ( int i = 0; i < nodesLength; i++) {
			if(nodes[i].equals(node)){
				int ind = (i-n)%nodesLength;
				if(ind < 0) ind += nodesLength;
				returnVal = nodes[ind];
			}
		}
		return returnVal;
	}

	private class ServerTask extends AsyncTask<ServerSocket, MessageObj, Void> {
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					Socket socket = serverSocket.accept();
					ObjectInputStream objectInputStream =
							new ObjectInputStream(socket.getInputStream());
					MessageObj message = null;
					try {
						message = (MessageObj) objectInputStream.readObject();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}

					if (message.reqT.equals(MessageObj.reqType.INSERT)) {
                        /* String log2 = "Avd : " + thisNode + " received message " + message.key;
                        Log.v("Log2", log2); */
						Log.v(message.key, "r by" + String.valueOf(Integer.valueOf(thisNode) / 2));
						localDatabase.put(message.key, message.val);
					} else if(message.reqT.equals(MessageObj.reqType.QUERY_ONE)) {
							message.val = localDatabase.get(message.key);
							message.fwdPort = message.comingFrom;
							message.reqT = MessageObj.reqType.QUERY_ONE_DONE;
							newClient(message);
					} else if (message.reqT.equals(MessageObj.reqType.QUERY_ONE_DONE)) {
						if(message.key!=null && message.val != null) {
							globalStore.put(message.key, message.val);
						}
					} else if(message.reqT.equals(MessageObj.reqType.QUERY_ALL)) {
							message.store.putAll(localDatabase);
							message.reqT = MessageObj.reqType.QUERY_DONE;
							message.fwdPort = message.comingFrom;
							newClient(message);
					} else if(message.reqT.equals(MessageObj.reqType.QUERY_DONE)) {
						globalStore.putAll(message.store);
						queryReceivedFrom++;
						if(queryReceivedFrom == 4) {
							wait = false;
							queryReceivedFrom = 0;
						}
					} else if(message.reqT.equals(MessageObj.reqType.DELETE)) {
						if(message.key.equals("*")) {
								localDatabase.clear();
						} else {
								localDatabase.remove(message.key);
						}
					} else if(message.reqT.equals(MessageObj.reqType.RECOVERY)) {
						Log.v("port recovering", thisNode);
						for (Map.Entry<String, String> entry : localDatabase.entrySet()) {
							if(belongsTo(entry.getKey(), message.comingFrom)) {
								message.store.put(entry.getKey(), entry.getValue());
							}
						}
						message.fwdPort = message.comingFrom;
						message.reqT =  MessageObj.reqType.RECOVER_DONE;
						newClient(message);
					} else if(message.reqT.equals((MessageObj.reqType.RECOVER_DONE))) {
						localDatabase.putAll(message.store);
					}
				}

			} catch (IOException e) {
				Log.e(TAG, "Server IO Exception");
			}
			return null;
		}
	}

	public boolean belongsTo(String key, String recoverPort) {
		String port = belongsWhere(key);
		String[] replicators = {recoverPort, returnNodePred(recoverPort, 1), returnNodePred(recoverPort, 2)};
		if(port.equals(recoverPort)) {
			return true;
		} else if(port.equals(returnNodePred(recoverPort, 1))) {
			return true;
		} else if(port.equals(returnNodePred(recoverPort, 2))) {
			return true;
		} else {
			return false;
		}
	}

	private class ClientTask extends AsyncTask<MessageObj, Void, Void> {
		@Override
		protected Void doInBackground(MessageObj... messages) {
			MessageObj mess = messages[0];
			try {

				Log.v("sending "+mess.key, " to "+String.valueOf(Integer.valueOf(mess.fwdPort)/2));
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(mess.fwdPort));
				//String log3 = "Avd " + thisNode + " forwards : " + mess.key + " to avd - " + mess.fwdPort + " with type " + mess.reqT;
				//Log.v("log3", log3);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(
						socket.getOutputStream());
				objectOutputStream.writeObject(mess);
				objectOutputStream.close();
				socket.close();

			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException" + e.getMessage());
			} catch (IOException e) {
			}
			return null;
		}
	}
}

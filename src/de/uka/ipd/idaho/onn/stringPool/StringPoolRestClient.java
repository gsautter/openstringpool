/* RefBank, the distributed platform for bibliographic references.
 * Copyright (C) 2011-2013 ViBRANT (FP7/2007-2013, GA 261532), by D. King & G. Sautter
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package de.uka.ipd.idaho.onn.stringPool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;

import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;

/**
 * Java implementation of a client for the parsed string pool protocol. Sub
 * classes willing to use their own XML namespace instead of the default one
 * should overwrite all respective methods.
 * 
 * @author sautter
 */
public class StringPoolRestClient implements StringPoolClient, StringPoolConstants, StringPoolXmlSchemaProvider {
	
	private String xmlNamespaceAttribute = this.getNamespaceAttribute();
	private String stringSetNodeType = this.getStringSetNodeType();
	private String stringNodeType = this.getStringNodeType();
	private String stringPlainNodeType = this.getStringPlainNodeType();
	private String stringParsedNodeType = this.getStringParsedNodeType();
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolXmlSchemaProvider#getNamespaceAttribute()
	 */
	public String getNamespaceAttribute() {
		return SP_XML_NAMESPACE_ATTRIBUTE;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolXmlSchemaProvider#getStringNodeType()
	 */
	public String getStringNodeType() {
		return STRING_NODE_TYPE;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolXmlSchemaProvider#getStringParsedNodeType()
	 */
	public String getStringParsedNodeType() {
		return STRING_PARSED_NODE_TYPE;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolXmlSchemaProvider#getStringPlainNodeType()
	 */
	public String getStringPlainNodeType() {
		return STRING_PLAIN_NODE_TYPE;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolXmlSchemaProvider#getStringSetNodeType()
	 */
	public String getStringSetNodeType() {
		return STRING_SET_NODE_TYPE;
	}
	
	private static class PooledStringRC extends PooledString {
		private String canonicalId = null;
		private String stringPlain = null;
		private String stringParsed = null;
		private String parseChecksum = null;
		private String parseError = null;
		private long createTime = -1;
		private String createDomain = null;
		private String createUser = null;
		private long updateTime = -1;
		private String updateDomain = null;
		private String updateUser = null;
		private long nodeUpdateTime = -1;
		private boolean created = false;
		private boolean updated = false;
		private boolean deleted = false;
		private PooledStringRC(String id) {
			super(id);
		}
		public String getStringPlain() {
			return this.stringPlain;
		}
		public String getStringParsed() {
			return this.stringParsed;
		}
		public String getCanonicalStringID() {
			return ((this.canonicalId == null) ? this.id : this.canonicalId);
		}
		public String getParseChecksum() {
			return this.parseChecksum;
		}
		public String getParseError() {
			return this.parseError;
		}
		public long getCreateTime() {
			return this.createTime;
		}
		public String getCreateDomain() {
			return this.createDomain;
		}
		public String getCreateUser() {
			return this.createUser;
		}
		public long getUpdateTime() {
			return this.updateTime;
		}
		public String getUpdateDomain() {
			return this.updateDomain;
		}
		public String getUpdateUser() {
			return this.updateUser;
		}
		public long getNodeUpdateTime() {
			return this.nodeUpdateTime;
		}
		public boolean wasCreated() {
			return this.created;
		}
		public boolean wasUpdated() {
			return this.updated;
		}
		public boolean isDeleted() {
			return this.deleted;
		}
	}
	
	private class ThreadedPSI implements PooledStringIterator {
		private final boolean debugThreading = false;
		private IOException ioe;
		private PooledString next;
		private Thread parser;
		private Object parserLock = new Object();
		private Reader reader;
		private ThreadedPSI(Reader r) {
			this.reader = r;
			if (debugThreading) System.out.println("StringPoolRestClient: creating threaded iterator");
			this.parser = new Thread() {
				public void run() {
					if (debugThreading) System.out.println("StringPoolRestClient: parser thread starting to run");
					synchronized(parserLock) {
						if (debugThreading) System.out.println("StringPoolRestClient: parser thread waking up creator");
						parserLock.notify();
						if (debugThreading) System.out.println("StringPoolRestClient: parser thread creator woken up");
					}
					try {
						xmlParser.stream(reader, new TokenReceiver() {
							private StringBuffer stringPlainBuffer = null;
							private String stringPlain = null;
							private StringBuffer stringParsedBuffer = null;
							private LinkedList stringParsedTagStack = null;
							private String stringParsed = null;
							private PooledStringRC ps = null;
							public void close() throws IOException {}
							public void storeToken(String token, int treeDepth) throws IOException {
								if (xmlGrammar.isTag(token)) {
									String type = xmlGrammar.getType(token);
									type = type.substring(type.indexOf(':') + 1);
									boolean isEndTag = xmlGrammar.isEndTag(token);
									
									//	need to check stack first in case parsed string contains any of our functional tags
									if ((this.stringParsedBuffer != null) && (this.stringParsedTagStack != null)) {
										if (isEndTag) {
											if ((this.stringParsedTagStack.size() != 0) && this.stringParsedTagStack.getLast().equals(type)) {
												this.stringParsedBuffer.append(token);
												this.stringParsedTagStack.removeLast();
												return;
											}
										}
										else {
											this.stringParsedBuffer.append(token);
											if (!xmlGrammar.isSingularTag(token))
												this.stringParsedTagStack.addLast(type);
											return;
										}
									}
									
									//	start or end of pooled string proper
									if (stringNodeType.equals(type)) {
										if (isEndTag) {
											if (this.ps != null) {
												this.ps.stringPlain = this.stringPlain;
												this.ps.stringParsed = this.stringParsed;
											}
											synchronized(parserLock) {
												if (debugThreading) System.out.println("StringPoolRestClient: parser thread got next");
												next = this.ps;
												this.ps = null;
												if (debugThreading) System.out.println("StringPoolRestClient: parser thread waking up requester");
												parserLock.notify();
												if (debugThreading) System.out.println("StringPoolRestClient: parser thread woke up requester");
												try {
													if (debugThreading) System.out.println("StringPoolRestClient: parser thread going to sleep");
													parserLock.wait();
													if (debugThreading) System.out.println("StringPoolRestClient: parser thread woken up by requester");
												} catch (InterruptedException ie) {}
											}
										}
										else {
											TreeNodeAttributeSet stringAttributes = TreeNodeAttributeSet.getTagAttributes(token, xmlGrammar);
											String stringId = stringAttributes.getAttribute(STRING_ID_ATTRIBUTE);
											if (stringId == null)
												return;
											this.ps = new PooledStringRC(stringId);
											this.ps.canonicalId = stringAttributes.getAttribute(CANONICAL_STRING_ID_ATTRIBUTE);
											this.ps.createTime = parseTime(stringAttributes.getAttribute(CREATE_TIME_ATTRIBUTE, "-1"));
											this.ps.createDomain = stringAttributes.getAttribute(CREATE_DOMAIN_ATTRIBUTE);
											this.ps.createUser = stringAttributes.getAttribute(CREATE_USER_ATTRIBUTE);
											this.ps.updateTime = parseTime(stringAttributes.getAttribute(UPDATE_TIME_ATTRIBUTE, "-1"));
											this.ps.updateDomain = stringAttributes.getAttribute(UPDATE_DOMAIN_ATTRIBUTE);
											this.ps.updateUser = stringAttributes.getAttribute(UPDATE_USER_ATTRIBUTE);
											this.ps.nodeUpdateTime = parseTime(stringAttributes.getAttribute(LOCAL_UPDATE_TIME_ATTRIBUTE, "-1"));
											this.ps.deleted = "true".equalsIgnoreCase(stringAttributes.getAttribute(DELETED_ATTRIBUTE));
											this.ps.parseChecksum = stringAttributes.getAttribute(PARSE_CHECKSUM_ATTRIBUTE);
											this.ps.parseError = stringAttributes.getAttribute(PARSE_ERROR_ATTRIBUTE);
											this.ps.created = "true".equalsIgnoreCase(stringAttributes.getAttribute(CREATED_ATTRIBUTE));
											this.ps.updated = "true".equalsIgnoreCase(stringAttributes.getAttribute(UPDATED_ATTRIBUTE));
											if (xmlGrammar.isSingularTag(token)) {
												synchronized(parserLock) {
													if (debugThreading) System.out.println("StringPoolRestClient: parser thread got next");
													next = this.ps;
													this.ps = null;
													if (debugThreading) System.out.println("StringPoolRestClient: parser thread waking up requester");
													parserLock.notify();
													if (debugThreading) System.out.println("StringPoolRestClient: parser thread woke up requester");
													try {
														if (debugThreading) System.out.println("StringPoolRestClient: parser thread going to sleep");
														parserLock.wait();
														if (debugThreading) System.out.println("StringPoolRestClient: parser thread woken up by requester");
													} catch (InterruptedException ie) {}
												}
											}
										}
										this.stringPlainBuffer = null;
										this.stringPlain = null;
										this.stringParsedBuffer = null;
										this.stringParsedTagStack = null;
										this.stringParsed = null;
										return;
									}
									
									//	start or end of plain string
									if (stringPlainNodeType.equals(type)) {
										if (isEndTag) {
											if (this.stringPlainBuffer.length() != 0)
												this.stringPlain = this.stringPlainBuffer.toString();
											this.stringPlainBuffer = null;
										}
										else this.stringPlainBuffer = new StringBuffer();
										return;
									}
									
									//	start or end of parsed string
									if (stringParsedNodeType.equals(type)) {
										if (isEndTag) {
											if (this.stringParsedBuffer.length() != 0)
												this.stringParsed = this.stringParsedBuffer.toString();
											this.stringParsedBuffer = null;
											this.stringParsedTagStack = null;
										}
										else {
											this.stringParsedBuffer = new StringBuffer();
											this.stringParsedTagStack = new LinkedList();
										}
										return;
									}
								}
								else if (this.stringPlainBuffer != null)
									this.stringPlainBuffer.append(xmlGrammar.unescape(token));
								else if (this.stringParsedBuffer != null)
									this.stringParsedBuffer.append(token.trim());
							}
						});
						reader.close();
					}
					catch (IOException tioe) {
						ioe = tioe;
					}
					finally {
						synchronized(parserLock) {
							if (debugThreading) System.out.println("StringPoolRestClient: parser thread waking up requester at and of input");
							parserLock.notify();
							if (debugThreading) System.out.println("StringPoolRestClient: parser thread woke up requester at end of input");
							parser = null;
						}
						try {
							reader.close();
						}
						catch (IOException tioe) {
							ioe = tioe;
						}
					}
				}
			};
			if (debugThreading) System.out.println("StringPoolRestClient: parser thread created");
			synchronized(this.parserLock) {
				if (debugThreading) System.out.println("StringPoolRestClient: starting parser thread");
				this.parser.start();
				if (debugThreading) System.out.println("StringPoolRestClient: parser thread started");
				try {
					if (debugThreading) System.out.println("StringPoolRestClient: waiting for parser thread");
					this.parserLock.wait();
					if (debugThreading) System.out.println("StringPoolRestClient: parser thread running");
				} catch (InterruptedException ie) {}
			}
		}
		public IOException getException() {
			return this.ioe;
		}
		public boolean hasNextString() {
			if (this.next != null)
				return true;
			synchronized(this.parserLock) {
				if (parser == null)
					return false;
				if (debugThreading) System.out.println("StringPoolRestClient: waking up parser thread");
				this.parserLock.notify();
				if (debugThreading) System.out.println("StringPoolRestClient: parser thread woken up");
				try {
					if (debugThreading) System.out.println("StringPoolRestClient: waiting on parser thread to produce next");
					this.parserLock.wait();
					if (debugThreading) System.out.println("StringPoolRestClient: parser thread returned from quest for next");
				} catch (InterruptedException ie) {}
			}
			if (this.next == null) {
				while (this.parser != null)
					synchronized(this.parserLock) {
						if (debugThreading) System.out.println("StringPoolRestClient: waking up parser thread to terminate");
						this.parserLock.notify();
						if (debugThreading) System.out.println("StringPoolRestClient: parser thread woken up to terminate");
						try {
							if (debugThreading) System.out.println("StringPoolRestClient: waiting on parser thread to terminate");
							this.parserLock.wait();
							if (debugThreading) System.out.println("StringPoolRestClient: parser thread terminated");
						} catch (InterruptedException ie) {}
					}
				return false;
			}
			else return true;
		}
		public PooledString getNextString() {
			PooledString next = this.next;
			this.next = null;
			return next;
		}
		protected void finalize() throws Throwable {
			if (this.parser == null)
				return;
			while (this.hasNextString())
				this.getNextString();
		}
	}
	
	private class ListPSI implements PooledStringIterator {
		private Iterator pbrit;
		private ListPSI(ArrayList pbrs) {
			this.pbrit = pbrs.iterator();
		}
		public IOException getException() {
			return null;
		}
		public boolean hasNextString() {
			return this.pbrit.hasNext();
		}
		public PooledString getNextString() {
			return (this.pbrit.hasNext() ? ((PooledString) this.pbrit.next()) : null);
		}
	}
	
	private String baseUrl;
	
	/**
	 * Constructor
	 * @param baseUrl the URL of the string pool node to connect to
	 */
	public StringPoolRestClient(String baseUrl) {
		this.baseUrl = baseUrl;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#getString(java.lang.String)
	 */
	public PooledString getString(String stringId) throws IOException {
		String[] stringIds = {stringId};
		PooledStringIterator stringIt = this.getStrings(stringIds);
		if (stringIt.hasNextString())
			return stringIt.getNextString();
		IOException ioe = stringIt.getException();
		if (ioe == null)
			return null;
		throw ioe;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#getStrings(java.lang.String[])
	 */
	public PooledStringIterator getStrings(String[] stringIds) {
		try {
			StringBuffer stringIdString = new StringBuffer();
			for (int i = 0; i < stringIds.length; i++)
				stringIdString.append("&" + STRING_ID_ATTRIBUTE + "=" + URLEncoder.encode(stringIds[i], ENCODING));
			return this.receiveStrings(ACTION_PARAMETER + "=" + GET_ACTION_NAME + stringIdString.toString());
		}
		catch (IOException ioe) {
			return new ExceptionPSI(ioe);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#getLinkedStrings(java.lang.String)
	 */
	public PooledStringIterator getLinkedStrings(String canonicalStringId) throws IOException {
		try {
			return this.receiveStrings(ACTION_PARAMETER + "=" + GET_ACTION_NAME + "&" + CANONICAL_STRING_ID_ATTRIBUTE + "=" + URLEncoder.encode(canonicalStringId, ENCODING));
		}
		catch (IOException ioe) {
			return new ExceptionPSI(ioe);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#findStrings(java.lang.String[], boolean, java.lang.String, java.lang.String, int, boolean)
	 */
	public PooledStringIterator findStrings(String[] textPredicates, boolean disjunctive, String type, String user, int limit, boolean selfCanonicalOnly) {
		return this.findStrings(textPredicates, disjunctive, type, user, false, limit, selfCanonicalOnly);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#findStrings(java.lang.String[], boolean, java.lang.String, java.lang.String, boolean, int, boolean)
	 */
	public PooledStringIterator findStrings(String[] textPredicates, boolean disjunctive, String type, String user, boolean concise, int limit, boolean selfCanonicalOnly) {
		return this.findStrings(textPredicates, disjunctive, type, user, concise, limit, selfCanonicalOnly, null);
	}
	
	/**
	 * Find strings using full text search plus sub class specific detail
	 * queries. This method is meant to be called by sub classes only.
	 * @param textPredicates the full text predicates
	 * @param disjunctive combine the predicates with 'or'?
	 * @param type the type of strings to search
	 * @param user the name of the user to contribute or last update the strings
	 * @param concise obtain a concise result, i.e., without parses?
	 * @param limit the maximum number of strings to include in the result (0
	 *            means no limit)
	 * @param selfCanonicalOnly filter out strings linked to others?
	 * @param detailPredicates the predicates to match against a sub class
	 *            specific index
	 * @return an iterator over the strings matching the query
	 */
	protected PooledStringIterator findStrings(String[] textPredicates, boolean disjunctive, String type, String user, boolean concise, int limit, boolean selfCanonicalOnly, String detailPredicates) {
		try {
			StringBuffer queryString = new StringBuffer();
			if (textPredicates != null) {
				for (int t = 0; t < textPredicates.length; t++)
					queryString.append("&" + QUERY_PARAMETER + "=" + URLEncoder.encode(textPredicates[t], ENCODING));
			}
			if (disjunctive)
				queryString.append("&" + COMBINE_PARAMETER + "=" + OR_COMBINE);
			if (type != null)
				queryString.append("&" + TYPE_PARAMETER + "=" + URLEncoder.encode(type, ENCODING));
			if (user != null)
				queryString.append("&" + USER_PARAMETER + "=" + URLEncoder.encode(user, ENCODING));
			if (concise)
				queryString.append("&" + FORMAT_PARAMETER + "=" + CONCISE_FORMAT);
			if (limit > 0)
				queryString.append("&" + LIMIT_PARAMETER + "=" + limit);
			if (selfCanonicalOnly)
				queryString.append("&" + SELF_CANONICAL_ONLY_PARAMETER + "=" + SELF_CANONICAL_ONLY_PARAMETER);
			if ((detailPredicates != null) && (detailPredicates.length() != 0)) {
				if (!detailPredicates.startsWith("&"))
					queryString.append("&");
				queryString.append(detailPredicates);
			}
			return this.receiveStrings(ACTION_PARAMETER + "=" + FIND_ACTION_NAME + queryString.toString());
		}
		catch (IOException ioe) {
			return new ExceptionPSI(ioe);
		}
	}
	
	/**
	 * Produce an iterator over the pooled strings coming in as XML as the
	 * response to some URL query string appended to the base URL. The data is
	 * assumed to be encoded in UTF-8. This method is meant to be used by sub
	 * classes only.
	 * @param urlString the URL to fetch pooled strings from
	 * @return an iterator over the pooled strings coming from the argument URL
	 * @throws IOException
	 */
	protected PooledStringIterator receiveStrings(String urlQueryString) throws IOException {
		URL url = new URL(this.baseUrl + "?" + urlQueryString);
		return this.receiveStrings(new BufferedReader(new InputStreamReader(url.openStream(), ENCODING)));
	}
	
	/**
	 * Produce an iterator over the pooled strings coming in as XML from some
	 * reader. This method is meant to be used by sub classes only.
	 * @param r the reader whose data to decode into pooled strings
	 * @return an iterator over the pooled strings coming from the argument reader
	 * @throws IOException
	 */
	protected PooledStringIterator receiveStrings(Reader r) throws IOException {
		return new ThreadedPSI(r);
//		final ArrayList stringList = new ArrayList();
//		xmlParser.stream(r, new TokenReceiver() {
//			private StringBuffer stringPlainBuffer = null;
//			private String stringPlain = null;
//			private StringBuffer stringParsedBuffer = null;
//			private String stringParsed = null;
//			private PooledStringRC ps = null;
//			public void close() throws IOException {}
//			public void storeToken(String token, int treeDepth) throws IOException {
//				if (xmlGrammar.isTag(token)) {
//					String type = xmlGrammar.getType(token);
//					type = type.substring(type.indexOf(':') + 1);
//					boolean isEndTag = xmlGrammar.isEndTag(token);
//					if (stringNodeType.equals(type)) {
//						if (isEndTag) {
//							if (this.ps != null) {
//								this.ps.stringPlain = this.stringPlain;
//								this.ps.stringParsed = this.stringParsed;
//								stringList.add(this.ps);
//							}
//							this.ps = null;
//						}
//						else {
//							TreeNodeAttributeSet stringAttributes = TreeNodeAttributeSet.getTagAttributes(token, xmlGrammar);
//							String stringId = stringAttributes.getAttribute(STRING_ID_ATTRIBUTE);
//							if (stringId == null)
//								return;
//							this.ps = new PooledStringRC(stringId);
//							try {
//								this.ps.createTime = parseTime(stringAttributes.getAttribute(CREATE_TIME_ATTRIBUTE, "-1"));
//								this.ps.createDomain = stringAttributes.getAttribute(CREATE_DOMAIN_ATTRIBUTE);
//								this.ps.createUser = stringAttributes.getAttribute(CREATE_USER_ATTRIBUTE);
//								this.ps.updateTime = parseTime(stringAttributes.getAttribute(UPDATE_TIME_ATTRIBUTE, "-1"));
//								this.ps.updateDomain = stringAttributes.getAttribute(UPDATE_DOMAIN_ATTRIBUTE);
//								this.ps.updateUser = stringAttributes.getAttribute(UPDATE_USER_ATTRIBUTE);
//								this.ps.nodeUpdateTime = parseTime(stringAttributes.getAttribute(LOCAL_UPDATE_TIME_ATTRIBUTE, "-1"));
//								this.ps.deleted = "true".equalsIgnoreCase(stringAttributes.getAttribute(DELETED_ATTRIBUTE));
//							}
//							catch (NumberFormatException nfe) {
//								System.out.println("Error in timestamps, token is " + token);
//								nfe.printStackTrace(System.out);
//								this.ps = null;
//								return;
//							}
//							this.ps.canonicalId = stringAttributes.getAttribute(CANONICAL_STRING_ID_ATTRIBUTE);
//							this.ps.parseChecksum = stringAttributes.getAttribute(PARSE_CHECKSUM_ATTRIBUTE);
//							this.ps.parseError = stringAttributes.getAttribute(PARSE_ERROR_ATTRIBUTE);
//							this.ps.created = "true".equalsIgnoreCase(stringAttributes.getAttribute(CREATED_ATTRIBUTE));
//							this.ps.updated = "true".equalsIgnoreCase(stringAttributes.getAttribute(UPDATED_ATTRIBUTE));
//							if (xmlGrammar.isSingularTag(token)) {
//								stringList.add(this.ps);
//								this.ps = null;
//							}
//						}
//						this.stringPlainBuffer = null;
//						this.stringPlain = null;
//						this.stringParsedBuffer = null;
//						this.stringParsed = null;
//					}
//					else if (stringPlainNodeType.equals(type)) {
//						if (isEndTag) {
//							if (this.stringPlainBuffer.length() != 0)
//								this.stringPlain = this.stringPlainBuffer.toString();
//							this.stringPlainBuffer = null;
//						}
//						else this.stringPlainBuffer = new StringBuffer();
//					}
//					else if (stringParsedNodeType.equals(type)) {
//						if (isEndTag) {
//							if (this.stringParsedBuffer.length() != 0)
//								this.stringParsed = this.stringParsedBuffer.toString();
//							this.stringParsedBuffer = null;
//						}
//						else this.stringParsedBuffer = new StringBuffer();
//					}
//					else if (this.stringParsedBuffer != null)
//						this.stringParsedBuffer.append(token);
//				}
//				else if (this.stringPlainBuffer != null)
//					this.stringPlainBuffer.append(xmlGrammar.unescape(token));
//				else if (this.stringParsedBuffer != null)
//					this.stringParsedBuffer.append(token.trim());
//			}
//		});
//		r.close();
//		return new ListPSI(stringList);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#getStringsUpdatedSince(long)
	 */
	public PooledStringIterator getStringsUpdatedSince(long updatedSince) {
		try {
			return this.receiveStrings(ACTION_PARAMETER + "=" + FEED_ACTION_NAME + "&" + UPDATED_SINCE_ATTRIBUTE + "=" + URLEncoder.encode(TIMESTAMP_DATE_FORMAT.format(new Date(Math.max(updatedSince, 1))), ENCODING));
		}
		catch (IOException ioe) {
			return new ExceptionPSI(ioe);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#getStringCount(long)
	 */
	public int getStringCount(long since) {
		try {
			URL countUrl = new URL(this.baseUrl + "?" + ACTION_PARAMETER + "=" + COUNT_ACTION_NAME + ((since < 1) ? "" : ("&" + SINCE_ATTRIBUTE + "=" + URLEncoder.encode(TIMESTAMP_DATE_FORMAT.format(new Date(since)), ENCODING))));
			final int[] count = {0};
			xmlParser.stream(new BufferedReader(new InputStreamReader(countUrl.openStream(), ENCODING)), new TokenReceiver() {
				public void storeToken(String token, int treeDepth) throws IOException {
					if (xmlGrammar.isTag(token) && !xmlGrammar.isEndTag(token) && stringSetNodeType.equals(xmlGrammar.getType(token))) {
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, xmlGrammar);
						String countString = tnas.getAttribute(COUNT_ATTRIBUTE);
						if (countString != null) try {
							count[0] = Integer.parseInt(countString);
						} catch (NumberFormatException nfe) {}
					}
				}
				public void close() throws IOException {}
			});
			return count[0];
		}
		catch (IOException ioe) {
			return 0;
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#getStringClusterCount(long)
	 */
	public int getStringClusterCount(long since) {
		try {
			URL countUrl = new URL(this.baseUrl + "?" + ACTION_PARAMETER + "=" + COUNT_ACTION_NAME + ((since < 1) ? "" : ("&" + SINCE_ATTRIBUTE + "=" + URLEncoder.encode(TIMESTAMP_DATE_FORMAT.format(new Date(since)), ENCODING))));
			final int[] count = {0};
			xmlParser.stream(new BufferedReader(new InputStreamReader(countUrl.openStream(), ENCODING)), new TokenReceiver() {
				public void storeToken(String token, int treeDepth) throws IOException {
					if (xmlGrammar.isTag(token) && !xmlGrammar.isEndTag(token) && stringSetNodeType.equals(xmlGrammar.getType(token))) {
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, xmlGrammar);
						String countString = tnas.getAttribute(CLUSTER_COUNT_ATTRIBUTE);
						if (countString != null) try {
							count[0] = Integer.parseInt(countString);
						} catch (NumberFormatException nfe) {}
					}
				}
				public void close() throws IOException {}
			});
			return count[0];
		}
		catch (IOException ioe) {
			return 0;
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#updateString(java.lang.String, java.lang.String)
	 */
	public PooledString updateString(String stringPlain, String user) throws IOException {
		return this.updateString(new UploadString(stringPlain), user);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#updateString(java.lang.String, java.lang.String, java.lang.String)
	 */
	public PooledString updateString(String stringPlain, String stringParsed, String user) throws IOException {
		return this.updateString(new UploadString(stringPlain, stringParsed), user);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#updateString(de.uka.ipd.idaho.onn.stringPool.StringPoolClient.UploadString, java.lang.String)
	 */
	public PooledString updateString(UploadString string, String user) throws IOException {
		UploadString[] strings = {string};
		PooledStringIterator stringIt = this.updateStrings(strings, user);
		if (stringIt.hasNextString())
			return stringIt.getNextString();
		IOException ioe = stringIt.getException();
		if (ioe == null)
			return null;
		throw ioe;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#updateStrings(java.lang.String[], java.lang.String)
	 */
	public PooledStringIterator updateStrings(String[] stringsPlain, String user) {
		UploadString[] strings = new UploadString[stringsPlain.length];
		for (int s = 0; s < stringsPlain.length; s++)
			strings[s] = new UploadString(stringsPlain[s]);
		return this.updateStrings(strings, user);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#updateStrings(de.uka.ipd.idaho.onn.stringPool.StringPoolClient.UploadString[], java.lang.String)
	 */
	public PooledStringIterator updateStrings(UploadString[] strings, String user) {
		try {
			URL putUrl = new URL(this.baseUrl);
			HttpURLConnection putCon = ((HttpURLConnection) putUrl.openConnection());
			putCon.setDoInput(true);
			putCon.setDoOutput(true);
			putCon.setRequestMethod("PUT");
			putCon.setRequestProperty("Data-Format", "xml");
			if (user != null)
				putCon.setRequestProperty("User-Name", user);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(putCon.getOutputStream(), ENCODING));
			bw.write("<" + this.stringSetNodeType + this.xmlNamespaceAttribute + ">");
			bw.newLine();
			for (int s = 0; s < strings.length; s++)
				this.writeXml(strings[s], bw);
			bw.write("</" + this.stringSetNodeType + ">");
			bw.newLine();
			bw.flush();
			bw.close();
			return this.receiveStrings(new BufferedReader(new InputStreamReader(putCon.getInputStream(), ENCODING)));
		}
		catch (IOException ioe) {
			return new ExceptionPSI(ioe);
		}
	}
	private void writeXml(UploadString string, BufferedWriter bw) throws IOException {
		bw.write("<" + this.stringNodeType + ">");
		bw.newLine();
		bw.write("<" + this.stringPlainNodeType + ">" + AnnotationUtils.escapeForXml(string.stringPlain) + "</" + this.stringPlainNodeType + ">");
		bw.newLine();
		if (string.stringParsed != null) {
			bw.write("<" + this.stringParsedNodeType + ">");
			bw.newLine();
			bw.write(string.stringParsed);
			bw.newLine();
			bw.write("</" + this.stringParsedNodeType + ">");
			bw.newLine();
		}
		bw.write("</" + this.stringNodeType + ">");
		bw.newLine();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#setCanonicalStringId(java.lang.String, java.lang.String, java.lang.String)
	 */
	public PooledString setCanonicalStringId(String stringId, String canonicalStringId, String user) throws IOException {
		return this.sendSimpleUpdate(stringId, canonicalStringId, false, user);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.onn.stringPool.StringPoolClient#setDeleted(java.lang.String, boolean, java.lang.String)
	 */
	public PooledString setDeleted(String stringId, boolean deleted, String user) throws IOException {
		return this.sendSimpleUpdate(stringId, null, deleted, user);
	}
	
	private PooledString sendSimpleUpdate(String stringId, String canonicalStringId, boolean deleted, String user) throws IOException {
		URL putUrl = new URL(this.baseUrl + "/" + UPDATE_ACTION_NAME);
		HttpURLConnection putCon = ((HttpURLConnection) putUrl.openConnection());
		putCon.setDoInput(true);
		putCon.setDoOutput(true);
		putCon.setRequestMethod("POST");
		putCon.setRequestProperty("Data-Format", "xml");
		if (user != null)
			putCon.setRequestProperty(USER_PARAMETER, user);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(putCon.getOutputStream(), ENCODING));
		bw.write("<" + this.stringSetNodeType + this.xmlNamespaceAttribute + ">");
		bw.newLine();
		bw.write("<" + this.stringNodeType);
		bw.write(" " + STRING_ID_ATTRIBUTE + "=\"" + AnnotationUtils.escapeForXml(stringId, true) + "\"");
		if (canonicalStringId != null)
			bw.write(" " + CANONICAL_STRING_ID_ATTRIBUTE + "=\"" + ((canonicalStringId == null) ? "" : canonicalStringId) + "\"");
		bw.write(" " + DELETED_ATTRIBUTE + "=\"" + (deleted ? "true" : "false") + "\"");
		bw.write("/>");
		bw.newLine();
		bw.write("</" + this.stringSetNodeType + ">");
		bw.newLine();
		bw.flush();
		bw.close();
		PooledStringIterator psi = this.receiveStrings(new BufferedReader(new InputStreamReader(putCon.getInputStream(), ENCODING)));
		return (psi.hasNextString() ? psi.getNextString() : null);
	}

	private static final long parseTime(String timeString) throws NumberFormatException {
		try {
			return TIMESTAMP_DATE_FORMAT.parse(timeString).getTime();
		}
		catch (ParseException pe) {
			return Long.parseLong(timeString);
		}
		catch (Exception e) {
			System.out.println("Could not parse time string |" + timeString + "|");
			throw new NumberFormatException(timeString);
		}
	}
//	
//	public static void main(String[] args) throws Exception {
////		if (true) {
//////			System.out.println(parseTime("Sat, 15 Oct 2011 19:04:19 +0000"));
////			System.out.println(parseTime("Sat, 15 Oct 2011 19:04:19 +0000"));
////			return;
////		}
//		
//		//	TODO test threaded iterator after nodes populated
//		StringPoolRestClient pbrc = new StringPoolRestClient("http://plazi2.cs.umb.edu/RefBank/rbk") {
//			public String getNamespaceAttribute() {
//				return "";
//			}
//			public String getStringNodeType() {
//				return "ref";
//			}
//			public String getStringParsedNodeType() {
//				return "refParsed";
//			}
//			public String getStringPlainNodeType() {
//				return "refString";
//			}
//			public String getStringSetNodeType() {
//				return "refSet";
//			}
//		};
////		PooledStringIterator pbri = pbrc.findStrings(null, false, null, "Agosti", null, -1, null, true);
//		PooledStringIterator pbri = pbrc.getStringsUpdatedSince(1);
//		while (pbri.hasNextString()) {
//			PooledString pbr = pbri.getNextString();
//			System.out.println(pbr.id + ": " + pbr.getStringPlain());
//			String stringParsed = pbr.getStringParsed();
//			if (stringParsed != null) {
//				System.out.println("  parsed: " + stringParsed);
////				UploadString ur = new UploadString(ps.getStringString(), ps.getStringParsed());
////				PooledString upbr = pbrc.updateString(ur);
////				if (upbr.wasUpdated())
////					System.out.println("  updated (" + upbr.getParseChecksum() + ")");
////				BufferedWriter urw = new BufferedWriter(new PrintWriter(System.out));
////				urw.writeXml(urw);
////				urw.flush();
//			}
//			String stringParseChecksum = pbr.getStringParsed();
//			if (stringParseChecksum != null)
//				System.out.println("  parse checksum: " + stringParseChecksum);
//		}
//		IOException ioe = pbri.getException();
//		if (ioe != null)
//			System.out.println(ioe.getMessage());
//	}
}

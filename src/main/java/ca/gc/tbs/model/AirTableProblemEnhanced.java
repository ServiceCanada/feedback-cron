package ca.gc.tbs.model;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.SerializedName;

public class AirTableProblemEnhanced {
	// let blank for airtable
	private String id;

	@SerializedName("Unique ID")
	private String uniqueID;
	@SerializedName("Date")
	private String date;
	@SerializedName("URL")
	private String URL;
	
	@SerializedName("Time received")
	private String timeStamp;
	
	@SerializedName("UTM URL")
	private String UTM;
	@SerializedName("URL_link")
	private List<String> URLLinkIds = new ArrayList<String>();

	@SerializedName("pageTitle")
	private String pageTitle;
	@SerializedName("Lang")
	private String lang;
	@SerializedName("What's wrong")
	private String whatswrong;
	@SerializedName("Comment")
	private String comment;
	@SerializedName("Ignore")
	private String ignore;
	@SerializedName("Tags")
	private List<String> tags= new ArrayList<String>();
	@SerializedName("Tags confirmed")
	private String tagsConfirmed;
	@SerializedName("Refining details")
	private String refiningDetails;
	@SerializedName("Actionable")
	private String actionable;
	@SerializedName("Main section")
	private String mainSection;
	@SerializedName("Status")
	private String status;
	@SerializedName("Institution")
	private String institution;
	@SerializedName("Theme")
	private String theme;
	
	

	@SerializedName("Lookup_tags")
	private String lookupTags;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUniqueID() {
		return uniqueID;
	}

	public void setUniqueID(String uniqueID) {
		this.uniqueID = uniqueID;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
	public String getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public String getWhatswrong() {
		return whatswrong;
	}

	public void setWhatswrong(String whatswrong) {
		this.whatswrong = whatswrong;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getIgnore() {
		return ignore;
	}

	public void setIgnore(String ignore) {
		this.ignore = ignore;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getTagsConfirmed() {
		return tagsConfirmed;
	}

	public void setTagsConfirmed(String tagsConfirmed) {
		this.tagsConfirmed = tagsConfirmed;
	}

	public String getRefiningDetails() {
		return refiningDetails;
	}

	public void setRefiningDetails(String refiningDetails) {
		this.refiningDetails = refiningDetails;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getLookupTags() {
		return lookupTags;
	}

	public void setLookupTags(String lookupTags) {
		this.lookupTags = lookupTags;
	}

	public String getURL() {
		return URL;
	}

	public void setURL(String uRL) {
		URL = uRL;
	}

	public String getUTM() {
		return UTM;
	}

	public void setUTM(String uTM) {
		UTM = uTM;
	}

	public String getActionable() {
		return actionable;
	}

	public void setActionable(String actionable) {
		this.actionable = actionable;
	}

	public String getMainSection() {
		return mainSection;
	}

	public void setMainSection(String mainSection) {
		this.mainSection = mainSection;
	}

	public String getInstitution() {
		return institution;
	}

	public void setInstitution(String institution) {
		this.institution = institution;
	}

	public String getTheme() {
		return theme;
	}

	public void setTheme(String theme) {
		this.theme = theme;
	}

	public String getPageTitle() {
		return pageTitle;
	}

	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}
	public List<String> getURLLinkIds() {
		return URLLinkIds;
	}

	public void setURLLinkIds(List<String> URLLinkIds) {
		this.URLLinkIds = URLLinkIds;
	}


}

# AppliedOntology_02

I have been working on a prosopographical research tool that requires a sequence of multiple tools. I am unconvinced that my current workflow is adequately completing the task.

# Career-events
We are studying career trajectories of international elites. In particular, we are interested in the geographic, organizational, and functional mobility. 

To understand trajectories, we are focused on collecting data about individual's "career-events" from unstructured text. A "career-event" is the atomic unit in someone's career. A career-event consists of: 

- organization: the organization where the career-event took place
- time: time descriptors of the career event
- function: the person's title or role 
- location: the location(s) of the career event. 

Career-events can be overlapping; for example, a diplomat who is posted in the United States could be both the representative to the UN AND the ambassador to the United States. These would be two career-events, taking place simultaneously. They would be in the same organization, could be in the same location, and at the same time, but refer to two different functions.  

# Challenge
- A primary problem is that organizations and roles are referenced in inconsistent ways in different texts. This makes our results messy and partially overlapping. 
- Another challenge is that the texts we are looking at are very different. Some are CVs that are densely packed with career-events. Others are not.

# Tools
I have been addressing this with two tools: 
- LLM tools that work with unstructured text, try to reduce redundancy and partial overlap, work with standardized output structures 
- An organizational ontology tool that helps to classify organizations for consistency. 

# Task
I need to take a step back and make sure that all of the tools are working individually. I need to have more human evaluation and observation of the processes. I want to experiment with some different sequences. 
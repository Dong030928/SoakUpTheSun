package org.hgc.suts.gateway.memory;

import org.springframework.ai.chat.client.ChatClientRequest;
import org.springframework.ai.chat.client.ChatClientResponse;
import org.springframework.ai.chat.client.advisor.api.Advisor;
import org.springframework.ai.chat.client.advisor.api.AdvisorChain;
import org.springframework.ai.chat.client.advisor.api.BaseAdvisor;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;

import java.util.ArrayList;
import java.util.List;

/** Enrich only the current turn after memory capture and outside the tool loop. */
public final class ReferenceContextAdvisor implements BaseAdvisor {
    private final String input;

    public ReferenceContextAdvisor(String input) { this.input = input; }

    @Override
    public int getOrder() { return Advisor.DEFAULT_CHAT_MEMORY_PRECEDENCE_ORDER + 50; }

    @Override
    public ChatClientRequest before(ChatClientRequest request, AdvisorChain chain) {
        List<Message> messages = new ArrayList<>(request.prompt().getInstructions());
        for (int i = messages.size() - 1; i >= 0; i--) {
            if (messages.get(i) instanceof UserMessage) {
                messages.set(i, new UserMessage(input));
                break;
            }
        }
        return request.mutate().prompt(request.prompt().mutate().messages(messages).build()).build();
    }

    @Override
    public ChatClientResponse after(ChatClientResponse response, AdvisorChain chain) { return response; }
}
